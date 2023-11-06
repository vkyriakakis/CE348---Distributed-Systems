import socket
import struct
import threading
import random
import time

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


class RRClient:
	def __init__(self, server_ip, server_port, resend_time = 2, ploss = 0):
		"Initializes the client-side request-reply middleware"

		# Initialize UDP socket
		self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		self._sock.bind(("", 0))
		
		# Server address
		self._addr = (server_ip, server_port)

		# Packet loss probability
		self._ploss = ploss

		# Time between successive retransmissions of requests
		self._resend_time = resend_time

		# Metrics to be provided to the app, namely the number
		# of RR interactions, and the total amount of bytes sent over
		# the network (client -> server and the reverse)
		self._rr_count = 0
		self._total_data = 0
		self._ack_repl_data = 0
		self._req_data = 0
		self._repl_data = 0
		self._ack_req_data = 0

		# Next sequence number to be assigned to a request (used
		# by the client to drop duplicate replies), also the next expected
		# reply seqno (Stop & Wait)
		self._seqno = 0

		# Flag concerning the receipt of the expected ACK, used
		# to change the handling of retransmissions
		self._is_ack_recv = False

		# The expected reply is stored here, so that
		# the thread waiting at req_reply() will find it
		self._reply = None

		# Binary semaphore used by the req_reply() caller
		# to wait until an event (ACK receipt or REPLY receipt)
		# happened
		self._event_wait = threading.Semaphore(value = 0)

		# Thread that receives stuff from the server and notifies
		# a thread stuck at req_reply about it (e.g. an ack or the expected reply)
		recv_thr = threading.Thread(target = self._run_recv)
		recv_thr.daemon = True
		recv_thr.start()

	def req_reply(self, request):
		"""Sends request to the server as many times as needed so that the reply
		   is received, and returns the reply."""

		# Packet tags
		global REQ

		# Form the request message
		req_len = len(request)
		req_msg = _serialize((REQ, self._seqno, req_len, request))

		# Send request packet to server
		if not _is_packet_lost(self._ploss):
			self._sock.sendto(req_msg, self._addr)
		self._total_data += len(req_msg)
		self._req_data += len(req_msg)

		while True:
			# Wait for an event until timeout (timeout depends on state)
			if self._is_ack_recv:
				event_happ = self._event_wait.acquire(timeout = self._resend_time*10)
				self._is_ack_recv = False
			else:
				event_happ = self._event_wait.acquire(timeout = self._resend_time)

			# If there was a timeout, retransmit
			if not event_happ and not self._is_ack_recv:
				# Print for debug
				if not self._is_ack_recv:
					pass#print("RETR BEFORE ACK")
				else:
					#print("RETR AFTER ACK")
					self._is_ack_recv = False

				if not _is_packet_lost(self._ploss):
					self._sock.sendto(req_msg, self._addr)
				self._total_data += len(req_msg)
				self._req_data += len(req_msg)

			# If there was a reply
			elif self._reply is not None:
				break

		# Reset ack recv flag for next RR
		self._is_ack_recv = False

		# Empty reply storage, after storing the reply
		reply = self._reply
		self._reply = None

		# Increment the RR interaction count
		self._rr_count += 1

		return reply

	def metrics(self):
		"Returns the tuple (RR count, total data over net)"
		return (self._rr_count, self._total_data, self._req_data,
			    self._repl_data, self._ack_req_data, self._ack_repl_data)

	def _run_recv(self):
		"Receive thread runner function."

		# Packet tags
		global REPLY
		global ACK_REQ
		global ACK_REPLY

		while True:
			buf, addr = self._sock.recvfrom(60000)
			
			# Only concern yourself with messages sent by the server
			if addr != self._addr:
				continue

			# Add to metric
			self._total_data += len(buf)

			# Do sth depending on the msg type
			msg = _deserialize(buf)
			if msg[0] == REPLY:
				# Send ACK-REPLY
				ack_reply = _serialize((ACK_REPLY, msg[1]))
				if not _is_packet_lost(self._ploss):
					self._sock.sendto(ack_reply, self._addr)

				# Update metrics
				self._total_data += len(ack_reply)
				self._ack_repl_data += len(ack_reply)
				self._repl_data += len(buf)

				# If the reply is the currently expected
				# (reply.seqno == self._seqno), store it and alert
				# a thread waiting at req_reply, also update the next seqno
				if self._seqno == msg[1]:
					#print(msg[1])
					self._reply = msg[3]
					self._seqno += 1
					self._event_wait.release()

			elif msg[0] == ACK_REQ:
				self._ack_req_data += len(buf)

				# If the ACK_REQ corresponds to the
				# current request, change the state to ACK_RECV,
				# also notify the thread at req_reply, so that it
				# will be able to change the timout to that of ACK_RECV
				if msg[1] == self._seqno and not self._is_ack_recv:
					self._is_ack_recv = True
					self._event_wait.release()