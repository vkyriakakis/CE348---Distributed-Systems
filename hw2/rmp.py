import socket
import threading
import time
import struct
import random

# Reliable Message Passing implementation over UDP/IP
# to be used in distrsys hw2.

class RMP:
	# RMP packet types
	MSG = 0
	ACK = 1

	# Size arguments in message num
	# timeout in seconds
	def __init__(self, sbuf_size = 4080, rbuf_size = 4080, ip = "", port = 0, timeout = 2, ploss = 0,
                     porder = 2):
		# Send and receive buffer sizes
		self._sbuf_size = sbuf_size
		self._rbuf_size = rbuf_size

		# Timeout duration
		self._timeout = timeout

		# Packet loss probability
		self._ploss = ploss

		# Out of order probability
		self._porder = porder

		# Send buffer is a dictionary
		# indexed by seqno, that contains
		# (ser_msg, addr, last_send_time).
		# Receive buffer is a list that contains
		# (addr, data).
		self._sbuf = {}
		self._rbuf = []

		# Received message set, used for 
		# duplicate detection, ids are (addr, seqnum)
		self._mids = []

		# Initial sequence number,
		# used for receiving acks and dropping
		# duplicates.
		random.seed()
		self._seqno = random.randint(0, 32768) # 0 to 2^15

		# Initialize the socket used for communication
		self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		self._sock.bind((ip, port))

		# Number of UDP messages sent
		self._udp_total = 0

		# Initialize buffer mutexes
		self._rbuf_mtx = threading.Lock()
		self._sbuf_mtx = threading.Lock()

		# Initialize condition variables
		self._sbuf_not_full = threading.Condition(self._sbuf_mtx)
		self._sbuf_not_empty = threading.Condition(self._sbuf_mtx)
		self._rbuf_not_empty = threading.Condition(self._rbuf_mtx)

		# Create receiver and retransmitter threads
		self._recv_thr = threading.Thread(target = self._run_recv)
		self._recv_thr.daemon = True
		self._send_thr = threading.Thread(target = self._run_send)
		self._send_thr.daemon = True

		# And start them
		self._recv_thr.start()
		self._send_thr.start()

	# Used to simulate packet loss
	def _is_packet_lost(self):
		# A ploss of 0 means no packet loss
		if self._ploss == 0:
			return False
		elif random.randint(1, self._ploss) == 1:
			#print("PKT LOSS")
			return True

		return False

	# Used to simulate out of order packet delivery
	def _is_out_order(self):
		# A porder of 0 means no out of order
		if self._porder == 0:
			return False
		elif random.randint(1, self._porder) == 1:
			#print("PKT LOSS")
			return True

		return False

	# addr is (IP, port)
	# send() is asynchronous
	def send(self, addr, data, size):
		# Enter the monitor
		self._sbuf_mtx.acquire()

		# Wait while the send buffer is full
		while len(self._sbuf) == self._sbuf_size:
			self._sbuf_not_full.wait()

		# Create MSG packet to be sent
		packet = (RMP.MSG, self._seqno, size, data)

		# Increment sequence number
		self._seqno += 1

		# Serialize the packet
		ser_msg = self._serialize(packet)

		# Insert packet into the send buffer so that
		# the sender thread can take care of it.
		self._sbuf[self._seqno - 1] = (ser_msg, addr, time.time())

		# Send the message to addr
		if not self._is_packet_lost():
			self._udp_total += 1
			self._sock.sendto(ser_msg, addr)

		# Send buf not empty anymore (for sender thread)
		self._sbuf_not_empty.notify()

		# Exit the monitor
		self._sbuf_mtx.release()

	# Returns a (sender_addr, data) tuple
	def recv(self):
		# Enter the recv buf monitor
		self._rbuf_mtx.acquire()

		# Wait while it is empty
		while len(self._rbuf) == 0:
			self._rbuf_not_empty.wait()

		# Remove the first (addr, data) tuple
		# in the buffer
		recv_msg = self._rbuf.pop(0)

		# Exit the monitor
		self._rbuf_mtx.release()

		return recv_msg

	def get_udp_total(self):
		return self._udp_total

	# Returns UDP port used by RMP
	def get_port(self):
		_, port = self._sock.getsockname()
		return port

	# Receiver thread code, it
	# receives msgs and does something depending
	# on the type
	def _run_recv(self):
		while True:
			# Receive a message from socket
			ser_packet, addr = self._sock.recvfrom(4096)

			# Deserialize the message
			packet = self._deserialize(ser_packet)

			# If it was an RMP-MSG packet and the recv buf isn't full
			if packet[0] == RMP.MSG:
				if len(self._rbuf) < self._rbuf_size:
					# If it isn't a duplicate
					if (addr, packet[1]) not in self._mids:
						# Enter recv buf monitor
						self._rbuf_mtx.acquire()

						# Insert it into the recv buffer
						self._rbuf.insert(0, (addr, packet[3]))

						# Notify that not empty anymore
						self._rbuf_not_empty.notify()

						# Insert into received message set
						self._mids.append((addr, packet[1]))

						# Exit monitor
						self._rbuf_mtx.release()
					else:
						pass#print("DUP {}".format(packet[1]))

					# Create ack packet for the message,
					# even if it was a duplicate (the ack might have
					# been lost)
					ack_packet = (RMP.ACK, packet[1])

					# Serialize ack packet
					ser_ack = self._serialize(ack_packet)

					# Send the ack packet
					if not self._is_packet_lost():
						self._udp_total += 1
						self._sock.sendto(ser_ack, addr)
				else:
					pass#print("RECBUF FULL")

			# Else if it was an ACK packet
			elif packet[0] == RMP.ACK:
				# Enter send buf monitor
				self._sbuf_mtx.acquire()

				# DEBUG
				#print("ACK {}".format(packet[1]))

				# Check if the corresponding msg (same seqno)
				# exists in the send buffer and is unacknowledged
				if packet[1] in self._sbuf:
					# It was acked, remove it from send buf
					del self._sbuf[packet[1]]

					# The send buffer is not full anymore
					self._sbuf_not_full.notify()

				# Exit monitor
				self._sbuf_mtx.release()

	# Sender thread code. This thread send the messages
	# in the send buffer to their destination, and retransmits them
	# until an ACK is received.
	def _run_send(self):
		while True:
			# Enter the send buffer monitor
			self._sbuf_mtx.acquire()

			# While the send buffer is empty, block
			while len(self._sbuf) == 0:
				self._sbuf_not_empty.wait()

			# Loop over the buffer once, and retransmit all timed
			# out messages
			for seqnum in self._sbuf:
				if time.time() - self._sbuf[seqnum][2] > self._timeout:
					#print("RETR {}".format(seqnum))
					if not self._is_packet_lost():
						self._udp_total += 1
						self._sock.sendto(self._sbuf[seqnum][0], self._sbuf[seqnum][1])

					# Set the new send time
					msg, addr, _ = self._sbuf[seqnum]
					self._sbuf[seqnum] = (msg, addr, time.time())

			# Exit the monitor
			self._sbuf_mtx.release()



	# Packet serialization and deserialziation routines
	def _serialize(self, packet):
		if packet[0] == RMP.MSG:
			buf = struct.pack("!BIH" + str(packet[2]) + "s", packet[0], 
				                                             packet[1], 
				                                             packet[2], 
				                                             packet[3])
		elif packet[0] == RMP.ACK:
			buf = struct.pack("!BI", packet[0], packet[1])

		return buf


	def _deserialize(self, buf):
		pack_type = struct.unpack_from("!B", buf, 0)
		if pack_type[0] == RMP.MSG:
			seqno, length = struct.unpack_from("!IH", buf, 1)
			data = struct.unpack_from("!" + str(length) + "s", buf, 7)
			return (pack_type[0], seqno, length, data[0])
		elif pack_type[0] == RMP.ACK:
			seqno = struct.unpack_from("!I", buf, 1)
			return (pack_type[0], seqno[0])


if __name__ == "__main__":
	option = str(input())

	# Sender has port 5555
	if option == "s":		
		rmp = RMP(sbuf_size = 3, rbuf_size = 3, port = 5555, ploss = 5)

		f = open("animeLis", "r")
		for line in f:
			rmp.send(("192.168.1.8", 4444), line.encode(), len(line))
	elif option == "s1":
		rmp = RMP(sbuf_size = 3, rbuf_size = 3, port = 5555, ploss = 5)

		f = open("animeLis1", "r")
		for line in f:
			rmp.send(("192.168.1.8", 4444), line.encode(), len(line))
	elif option == "s2":
		rmp = RMP(sbuf_size = 3, rbuf_size = 3, port = 6666, ploss = 5)

		f = open("animeLis2", "r")
		for line in f:
			rmp.send(("192.168.1.8", 4444), line.encode(), len(line))
	elif option == "sA":
		rmp = RMP(sbuf_size = 3, rbuf_size = 3, port = 5555, ploss = 5)

		f = open("animeLisA", "r")
		for line in f:
			rmp.send(("192.168.1.8", 4444), line.encode(), len(line))
	elif option == "sB":
		rmp = RMP(sbuf_size = 3, rbuf_size = 3, port = 6666, ploss = 5)

		f = open("animeLisB", "r")
		for line in f:
			rmp.send(("192.168.1.8", 4444), line.encode(), len(line))
	elif option == "sC":
		rmp = RMP(sbuf_size = 3, rbuf_size = 3, port = 7777, ploss = 5)

		f = open("animeLisC", "r")
		for line in f:
			rmp.send(("192.168.1.8", 4444), line.encode(), len(line))
	# Receiver has port 4444
	elif option == "r":
		rmp = RMP(sbuf_size = 3, rbuf_size = 1, port = 4444, ploss = 8)
		while True:
			_, msg = rmp.recv()
			print(msg.decode().rstrip())
