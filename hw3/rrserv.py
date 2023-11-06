import socket
import struct
import threading
import time
import random
import collections

# Request-reply protocol packet tags
REQ = 0
REPLY = 1
ACK_REQ = 2
ACK_REPLY = 3

def _is_packet_lost(ploss):
	"Used to simulate packet loss"

	# A ploss of 0 means no packet loss
	if ploss == 0:
		return False
	elif random.randint(1, ploss) == 1:
		return True

	return False

def _serialize(msg):
	# Packet tags
	global REQ
	global REPLY
	global ACK_REQ
	global ACK_REPLY

	if msg[0] in (REQ, REPLY):
		return struct.pack("!BIH" + str(msg[2]) + "s",
			               msg[0], msg[1], msg[2], msg[3])
	elif msg[0] in (ACK_REQ, ACK_REPLY):
		return struct.pack("!BI", msg[0], msg[1])

def _deserialize(buf):
	# Packet tags
	global REQ
	global REPLY
	global ACK_REQ
	global ACK_REPLY

	mtype = struct.unpack_from("!B", buf)[0]
	if mtype in (REQ, REPLY):
		seqno, data_len = struct.unpack_from("!IH", buf, 1)
		data = struct.unpack_from("!" + str(data_len) + "s", buf, 7)[0]
		return (mtype, seqno, data_len, data)
	elif mtype in (ACK_REQ, ACK_REPLY):
		seqno = struct.unpack_from("!I", buf, 1)[0]
		return (mtype, seqno)

class RRServer:
	"""Request-reply server class."""
	def __init__(self, ip = "192.168.1.5", port = 7777, recv_size = 1024, 
		         send_size = 1024, ploss = 0, resend_time = 2):
		# Initialize UDP socket
		self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		self._sock.bind((ip, port))

		# Receive and send buffers
		self._rbuf = []
		self._rsize = recv_size
		self._sbuf = {}
		self._ssize = send_size

		# Packet loss probability
		self._ploss = ploss

		# Retransmit timeout
		self._resend_time = resend_time

		# Tuple containing the seqno and client_addr of the
		# last received request, used to send the reply
		self._reply_info = ()

		# Receiver thread
		self._rmtx = threading.Lock()
		recv_thr = threading.Thread(target = self._run_recv)
		recv_thr.daemon = True

		# Sender thread
		self._smtx = threading.Lock()
		send_thr = threading.Thread(target = self._run_send)
		send_thr.daemon = True

		# Condition variables (producer-consumer)
		self._sbuf_not_full = threading.Condition(lock = self._smtx)
		self._sbuf_not_empty = threading.Condition(lock = self._smtx)
		self._rbuf_not_empty = threading.Condition(lock = self._rmtx)

		# Start the threads
		recv_thr.start()
		send_thr.start()

	def get_req(self):
		"""Returns a request message, and sets the reply info
		to that of the request."""
		self._rmtx.acquire()

		while not self._rbuf:
			self._rbuf_not_empty.wait()

		# Remove the first request and reply info
		# from the receive buffer
		request, self._reply_info = self._rbuf.pop(0)

		self._rmtx.release()

		return request

	def send_reply(self, reply):
		"""Sends a reply message to the last request returned
		by get_req()."""
		global REPLY

		self._smtx.acquire()

		while len(self._sbuf) == self._ssize:
			self._sbuf_not_full.wait()

		# Create a REPLY packet
		reply_msg = _serialize((REPLY, self._reply_info[0], len(reply), reply))

		# Insert it to the send buffer, together with the client address
		# and the initial transmission time (right now)
		self._sbuf[self._reply_info] = (reply_msg,  time.time())

		# Signal the sender thread that sth must be sent
		self._sbuf_not_empty.notify()

		self._smtx.release()

	def _run_send(self):
		"""Runner function for the sender thread."""
		
		while True:
			# Enter the send buffer monitor
			self._smtx.acquire()

			# While the send buffer is empty, block
			while not self._sbuf:
				self._sbuf_not_empty.wait()

			# Loop over the buffer once, and retransmit all timed
			# out replies
			for reply_info in self._sbuf:
				if time.time() > self._sbuf[reply_info][1]:
					if not _is_packet_lost(self._ploss):
						self._sock.sendto(self._sbuf[reply_info][0], reply_info[1])

					# Set the new send time
					self._sbuf[reply_info] = (self._sbuf[reply_info][0], 
						                      time.time() + self._resend_time)

			# Exit the monitor
			self._smtx.release()

			# Be nice
			time.sleep(0)


	def _run_recv(self):
		"""Runner function for the receiver thread."""
		global REQ
		global ACK_REQ
		global ACK_REPLY

		while True:
			msg, addr = self._sock.recvfrom(60000)

			# Deserialize the message
			msg = _deserialize(msg)

			#print(msg)

			# If it was an REQUEST packet and the recv buf isn't full
			if msg[0] == REQ:
				if len(self._rbuf) < self._rsize:
					# Enter recv buf monitor
					self._rmtx.acquire()

					# Insert (request, (seqno, addr)) into the recv buffer
					self._rbuf.append((msg[3], (msg[1], addr)))

					# Notify that not empty anymore
					self._rbuf_not_empty.notify()

					# Exit monitor
					self._rmtx.release()

					# Create ack-req packet for the message,
					# even if it was a duplicate (the ack might have
					# been lost)
					ack_req = _serialize((ACK_REQ, msg[1]))

					# Send the ack packet
					if not _is_packet_lost(self._ploss):
						self._sock.sendto(ack_req, addr)
				else:
					#print("RECBUF FULL")
					pass

			# Else if it was an ACK-reply packet
			elif msg[0] == ACK_REPLY:
				# Enter send buf monitor
				self._smtx.acquire()

				# DEBUG
				#print("ACK {}".format(packet[1]))

				# Check if the corresponding msg (same seqno)
				# exists in the send buffer and is unacknowledged
				if (msg[1], addr) in self._sbuf:
					# It was acked, remove it from send buf
					del self._sbuf[(msg[1], addr)]

					# The send buffer is not full anymore
					self._sbuf_not_full.notify()

				# Exit monitor
				self._smtx.release()