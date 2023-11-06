import socket
import random
import threading
import struct
import sys
import collections
import time
import errno
import rmp
import gcp
import utils

# Constants used by applications to distinguish between
# types of messages returned by grp_recv()
MSG = 0
JOIN = 1
LEAVE = 2
FAULT = 3

class DiscFailException(Exception):
	pass

class ConnTimeoutException(Exception):
	pass

class NonBlockException(Exception):
 	pass

class GC:
	def __init__(self, mcast_ip = "239.255.0.4", mcast_port = 8916, gm_port = 7777,
		         disc_timeout = 2, conn_timeout = 10, ploss = 0, bm_delay = 0.0,
		         is_verbose = False):
		# Whether to print extra msgs or not
		self._is_verbose = is_verbose

		# The delay between successive unicast send()s in
		# BM
		self._bm_delay = bm_delay

		# Try to discover a GM
		gm_ip = self._discover(mcast_ip, mcast_port, disc_timeout)

		print("Discovered {}".format(gm_ip))

		# Create TCP socket to communicate with GM
		self._gm_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

		# Set the connection timeout
		self._gm_sock.settimeout(conn_timeout)

		# Try to connect
		try:
			self._gm_sock.connect((gm_ip, gm_port))
		except socket.timeout:
			raise ConnTimeoutException()

		# Connection successful, disable timeout
		self._gm_sock.settimeout(None)

		# Dictionary indexed by gsock, contains
		# the current (from the MW's point of view)
		# group view for all the groups the process belongs to. 
		# Each view is a list of (addr, id) tuples and is used
		# in the group multicast.
		self._views = collections.defaultdict(list)

		# Gsock to grpname and the reverse mapping
		self._gsock_to_grp = {}
		self._grp_to_gsock = {}

		# Next gsock
		self._next_gsock = 0

		# Initialize TCP mutex (used for the
		# packets to not mix)
		self._tcp_mtx = threading.Lock()

		# Receive buffer: Stores messages that are to be delivered
		# to the application. To acheive gsock demuxing it is
		# a dictionary indexed by gsock.
		self._rbuf = collections.defaultdict(list)

		# Mutex that protects the recv buffer
		self._rbuf_mtx = threading.Lock()

		# Condition variable used to wait until a msg comes
		# to a gsock (grp_recv with block). Signalled when
		# a message in that group is received.
		self._rbuf_not_empty = threading.Condition(lock = self._rbuf_mtx)

		# Initialize reliable message passing middleware
		# initialize with packet loss probability ploss
		self._rmp = rmp.RMP(ploss = ploss)

		# UDP port used by rmp, must be told to any group you join
		self._rmp_port = self._rmp.get_port()

		# The internet address of this process, used
		# to avoid needless reliable multicast retransmissions
		self._my_addr = (self._gm_sock.getsockname()[0], self._rmp_port)

		# Dictionary indexed by group name, contains first seqnum for each group
		self._seqno = collections.defaultdict(int)

		# Initialize set of received message identifiers (in
		# the form (addr, seqno, acc_num)) used in duplicate detection.
		# Each group needs such a set so this is a dictionary of lists indexed
		# by grpname. 
		self._mids = collections.defaultdict(list)

		# Every group needs a message buffer where out of
		# FIFO order messages can be stored until their turn to be delivered
		# arrives. That is why the grpname indexed dictionary of lists
		# mbuf is used. Messages are stored in the form
		# (seqno, addr, acc_num, msg, id).
		self._mbuf = collections.defaultdict(list)

		# Every group needs a data structure in which the next expected
		# seqno for each member. The dictionary of dictinories (indexed by grpname and proc_addr)
		# _delivered is used for that reason. When the _delivered[group][pid] for a member
		# is not known it is None, and it can't be used to give messages
		self._delivered = collections.defaultdict(lambda: collections.defaultdict(lambda: None))

		# Message buffer where totally ordered messages stay until their
		# turn (in total order) comes. Then they are removed and stored
		# into the FIFO buffer mbuf. This buffer can store a message
		# without the total sequence num, a total sequence num without the message,
		# or their combination.
		# The general form of the tuples stored here is:
		# (seqno, addr, acc_num, app_msg, id, total_seqno)
		# where app_msg = None AND id = None or total_seqno = None can happen (not both)
		self._total_buf = collections.defaultdict(list)

		# Analogous to _delivered, but concerns the next expected
		# total order sequence number
		self._total_delivered = collections.defaultdict(lambda: collections.defaultdict(lambda: None))

		# For every group, am_coord = True -> You are that group's T.O coordinator
		#                             False -> You aren't the coordinator
		self._am_coord = collections.defaultdict(bool)

		# The next total sequence number to be assigned by the coordinator
		# only use this if you are the coordinator
		self._total_seqno = collections.defaultdict(int)

		# For each group, the mids for which a total order seqno
		# was received are saved. This is done to achieve TSEQ duplication
		# detection as duplicates are produced by the retransmissions
		# needed for reliable TSEQ multicast
		self._tseqs = collections.defaultdict(list)

		# Mutex used so that the handling of a message is
		# atomic relative to leave/fault view changes, and grp_leave()
		# calls (so that the state of a sending process of group
		# isn't deleted during the handling)
		self._msg_hand_mtx = threading.Lock()

		# Initialize and start GM recv thread
		_gm_recv_thr = threading.Thread(target = self._run_gm_recv)
		_gm_recv_thr.daemon = True
		_gm_recv_thr.start()

		# Initialize and start RMP recv thread
		_rmp_recv_thr = threading.Thread(target = self._run_rmp_recv)
		_rmp_recv_thr.daemon = True
		_rmp_recv_thr.start()

	# Tries to discover a GM service using UDP/IP multicast
	# if disc_timeout elapses it raises an exception, 
	# else it returns the discovered server's IP address
	def _discover(self, mcast_ip, mcast_port, disc_timeout):
		# First create a UDP/IP socket for GM discover
		disc_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		disc_sock.bind(("", 0))

		# Set recv timeout to 0.1
		disc_sock.settimeout(0.1)

		# Send the Discovery Request message
		disc_req = struct.pack("!B", gcp.DISC_REQ)
		disc_sock.sendto(disc_req, (mcast_ip, mcast_port))

		# Wait for the Discovery Reply message until disc timeout
		# or reception of a discovery reply.
		start_time = time.time()
		while time.time() - start_time < disc_timeout:
			try:
					msg, addr = disc_sock.recvfrom(1024)
					# Try to check the message only after
					# recvfrom() succeds
					if struct.unpack_from("!B", msg)[0] == gcp.DISC_REPLY:
						return addr[0]

			# Ignore the socket.timeout exception
			except socket.timeout:
				pass

		# Discovery attempt timedout, raise exception
		raise DiscFailException()

	# Multicast message msg to all members
	# of <grpname>
	def _basic_multicast(self, msg, grpname):
		#print("BM!")
		# An element of the member list for <grpname>
		# might be deleted while you iterate, so lock the loop
		for mem in self._views[grpname]:
			self._rmp.send(mem[0], msg, len(msg))
			# If the bm delay isn't zero, sleep
			if self._bm_delay:
				time.sleep(self._bm_delay)

	# RMP recv thread receives group communication messages
	# from the RMP middleware and acts upon them
	def _run_rmp_recv(self):
		while True:
			sender_addr, msgp = self._rmp.recv()

			if self._is_verbose:
				print("Received {} from {}!".format(msgp, sender_addr))

			# Depending on the message's type, do something
			msg_type = struct.unpack_from("!B", msgp)[0]
			if msg_type in (gcp.MSG, gcp.TMSG):
				self._handle_msg(msgp, msg_type)
			elif msg_type == gcp.INIT_SEQ:
				self._handle_init_seq(msgp, sender_addr)
			elif msg_type == gcp.TSEQ:
				self._handle_tseq(msgp)
			elif msg_type == gcp.INIT_TSEQ:
				self._handle_init_tseq(msgp)

	def _handle_init_tseq(self, msgp):
		init_tseq, grp_len = struct.unpack_from("!IB", msgp, 1)
		grpname = struct.unpack_from("!" + str(grp_len) + "s", msgp, 6)[0].decode()

		# total_buf is accessed here, so the msg_handle_mtx is used
		self._msg_hand_mtx.acquire()

		# The initial expected seqno of sender_addr
		# was just received
		self._total_delivered[grpname] = init_tseq

		# Delete all received messages with total_seqno
		# smaller than the initial expected from the total buffer
		for msg in self._total_buf[grpname][:]:
			if msg[5] is not None and msg[5] < init_tseq:
				self._total_buf[grpname].remove(msg)

		# Now that the expected seqno was received,
		# check whether some messages are now in order in the total buffer and can
		# be delivered
		self._check_total_buffer(grpname)

		self._msg_hand_mtx.release()

	# Handles the receipt of a (message_id, total_seqnum) pair
	def _handle_tseq(self, msgp):
		# Unpack the TSEQ message
		acc_num, seqno = struct.unpack_from("!HI", msgp, 1)
		orig_IP = socket.inet_ntop(socket.AF_INET, msgp[7:11])
		orig_port, grp_len = struct.unpack_from("!HB", msgp, 11)
		addr = (orig_IP, orig_port)
		grpname, total_seqno = struct.unpack_from("!" + str(grp_len) + "sI", msgp, 14)
		grpname = grpname.decode()

		self._msg_hand_mtx.acquire()

		# If you aren't in the group the message was sent to anymore
		# or the tseq update is a duplicate, drop it
		if grpname not in self._views or (addr, seqno, acc_num) in self._tseqs[grpname]:
			self._msg_hand_mtx.release()
			return

		# Insert the message into the TSEQ buffer for
		# the already received
		self._tseqs[grpname].append((addr, seqno, acc_num))

		# If you aren't the coordinator, retransmit it (to acheive
		# reliable multicast of TSEQ messages)
		if not self._am_coord[grpname]:
			self._basic_multicast(msgp, grpname)

		was_found = False

		# Search the total buffer for the corresponding message
		for k in range(0, len(self._total_buf[grpname])):
			# If you found it, combine the total sequence number with it
			# and check the total buffer for any in total order msgs
			tmsg = self._total_buf[grpname][k]
			if (tmsg[0], tmsg[1], tmsg[2]) == (seqno, addr, acc_num):
				self._total_buf[grpname][k] = (tmsg[0], tmsg[1], tmsg[2], tmsg[3], tmsg[4], total_seqno)
				#print("Check total buffer from seq")
				self._check_total_buffer(grpname)
				was_found = True
				break

		# If you didn't find it, insert the total seqno in the buffer without
		# the payload
		if not was_found:
			self._total_buf[grpname].append((seqno, addr, acc_num, None, None, total_seqno))

		self._msg_hand_mtx.release()



	# Handles the receipt of a group members
	# initial next expected seqnum
	def _handle_init_seq(self, msgp, sender_addr):
		seqno, grp_len = struct.unpack_from("!IB", msgp, 1)
		grpname = struct.unpack_from("!" + str(grp_len) + "s", msgp, 6)[0].decode()

		# mbuf is accessed here, so the msg_handle_mtx is used
		self._msg_hand_mtx.acquire()

		# The initial expected seqno of sender_addr
		# was just received
		self._delivered[grpname][sender_addr] = seqno

		# Delete all received messages with seqno
		# smaller than the initial expected from the buffer
		for msg in self._mbuf[grpname][:]:
			if msg[0] < seqno:
				self._mbuf[grpname].remove(msg)

		#print("Delivered: {}".format(self._delivered))
		#print("Mbuf: {}".format(self._mbuf))

		# Now that the expected seqno was received,
		# check whether some messages are now in order and can
		# be delivered
		self._check_buffer(grpname)

		self._msg_hand_mtx.release()


	# Handles (in the spirit of event deliver <MSG...>) a
	# received message
	def _handle_msg(self, msgp, msg_type):
		# Get seqno, the group name and the application message
		acc_num, seqno = struct.unpack_from("!HI", msgp, 1)
		orig_IP = socket.inet_ntop(socket.AF_INET, msgp[7:11])
		orig_port, grp_len = struct.unpack_from("!HB", msgp, 11)
		addr = (orig_IP, orig_port)
		grpname, msg_len = struct.unpack_from("!" + str(grp_len) + "sH", msgp, 14)
		app_msg = struct.unpack_from("!" + str(msg_len) + "s", msgp, 16 + grp_len)[0]
		grpname = grpname.decode()
		
		self._msg_hand_mtx.acquire()

		# If you aren't in the group the message is sent to anymore
		# or the message is a duplicate, drop it
		if grpname not in self._views or (addr, seqno, acc_num) in self._mids[grpname]:
			self._msg_hand_mtx.release()
			return

		#print("Got here")

		# Find the id corresponding to <addr> in <grpname>
		# if a process with address <addr> does not belong in the
		# group, ignore the message
		for mem in self._views[grpname]:
			#print("{} == {}".format(mem[0], addr))
			if mem[0] == addr:
				# Add the message id to mids, so that you'll remember
				# that you received it
				self._mids[grpname].append((addr, seqno, acc_num))

				# If the message was not originally transmitted by (You),
				# the sender might have crashed meaning that not all
				# group members will receive it. To avoid this, re-multicast
				# the message.
				if addr != self._my_addr:
					self._basic_multicast(msgp, grpname)

				# Insert the message into the FIFO buffer,
				# along with its type (totally ordered TMSG, or normal MSG)
				self._mbuf[grpname].append((seqno, addr, acc_num, app_msg, mem[1], msg_type))

				# It may be the next in FIFO order, so check the buffer
				self._check_buffer(grpname)						                
				self._msg_hand_mtx.release()

				return

		# If you never found that particular process
		# drop the message
		self._msg_hand_mtx.release()

	# Check if any messages are in order, and can be delivered to the application
	# must be called inside _msg_hand_mtx
	def _check_buffer(self, grpname):
		# Number of in order msgs found in a pass
		# over mbuf, initialized to 1 to enter the loop
		found = 1

		#print("Delivered: {}".format(self._delivered[grpname]))
		#print("Mbuf: {}".format(self._mbuf[grpname]))
		#print("Mids: {}".format(self._mids[grpname]))

		# If at least one in order msg (seqno, addr, payload, send_id) was found
		# more might have gotten in order so loop again
		while found > 0:
			found = 0
			for seqno, addr, acc_num, app_msg, send_id, msg_type in self._mbuf[grpname][:]:
				# If a message is found for whose sender you have received
				# the initial next expected seqnum and the msg's seqno
				# is the next expected seqnum, deliver it to the app or total
				# buffer depending on the type
				if seqno == self._delivered[grpname][addr]:
					if msg_type == gcp.MSG:
						# Insert a MSG-type message in the recv buffer.
						# An MSG-type message is of the form
						# <MSG, id, data>

						self._rbuf_not_empty.acquire()
						#print("Inserting {} into rbuf!".format((MSG, (send_id.decode(), app_msg))))
						self._rbuf[self._grp_to_gsock[grpname]].append((MSG, (send_id.decode(), app_msg)))
						#print(self._rbuf[self._grp_to_gsock[grpname]])
						# Notify someone blocked in grp_recv
						self._rbuf_not_empty.notifyAll()
						self._rbuf_not_empty.release()
					elif msg_type == gcp.TMSG:
						#print("Deliver to total buf")
						was_found = False

						# It is a totally ordered message, so try to find the
						# corresponding total sequence number first
						for k in range(0, len(self._total_buf[grpname])):
							tmsg = self._total_buf[grpname][k]
							if (tmsg[0], tmsg[1], tmsg[2]) == (seqno, addr, acc_num):
								# The total seqno was there so merge the (app_msg, id) with it
								self._total_buf[grpname][k] = (tmsg[0], tmsg[1], tmsg[2], 
									                           app_msg, send_id, tmsg[5])
								was_found = True
								break

						# If the corresponing tseq wasn't found,
						# just insert the payload into the _check_buffer
						if not was_found:
							self._total_buf[grpname].append((seqno, addr, acc_num, app_msg, send_id, None))

							# Also, if you are the coordinator, assign
							# and multicast the next total sequence number 
							# (not needed if you found the total seqnum in the for-loop,
							# it was assigned by a previous coordinator)
							if self._am_coord[grpname]:
								#print(self._mbuf[grpname])
								# msgp[7:11] is IP in bytes form
								ip_int = struct.unpack("!I", socket.inet_aton(addr[0]))[0]
								grp_len = len(grpname.encode())
								total_seq = struct.pack("!BHIIHB" + str(grp_len) + "sI",
									                    gcp.TSEQ,
									                    acc_num,
									                    seqno,
									                    ip_int,
									                    addr[1],
									                    grp_len,
									                    grpname.encode(),
									                    self._total_seqno[grpname])

								# Update next total seqno
								self._total_seqno[grpname] += 1

								# Send the TSEQ msg
								self._basic_multicast(total_seq, grpname)

					# Remove the delivered msg from mbuf
					self._mbuf[grpname].remove((seqno, addr, acc_num, app_msg, send_id, msg_type))

					# Increment the next expected seqnum
					self._delivered[grpname][addr] += 1

					# Increment number of delivered msgs for the loop
					found += 1

		# In the end, check the total buffer as the next in total order
		# msg might have been inserted
		self._check_total_buffer(grpname)


	# Analogous to _check_buffer, but checks _total_buffer,
	# and if a msg is in total order, it is moved to the FIFO buffer
	# (not delivered to app)
	def _check_total_buffer(self, grpname):
		found = 1

		#print("Total buffer: {}".format(self._total_buf[grpname]))
		while found > 0:
			found = 0
			for seqno, addr, acc_num, app_msg, send_id, total_seqno in self._total_buf[grpname][:]:
				# If the message is complete (has both total_seqno and payload),
				# and it is the next in total order, it is removed from total buffer
				# and delivered to the recv buffer
				if total_seqno is not None and app_msg is not None and \
				   total_seqno == self._total_delivered[grpname]:
					self._total_buf[grpname].remove((seqno, addr, acc_num, 
				   	                                   app_msg, send_id, total_seqno))
					
					# Deliver to recv buffer
					self._rbuf_not_empty.acquire()
					#print("Inserting {} into rbuf!".format((MSG, (send_id.decode(), app_msg))))
					self._rbuf[self._grp_to_gsock[grpname]].append((MSG, (send_id.decode(), app_msg)))
					# Notify someone blocked in grp_recv
					#print(self._rbuf[self._grp_to_gsock[grpname]])
					self._rbuf_not_empty.notifyAll()
					self._rbuf_not_empty.release()

					# Update the next expected total sequence number
					self._total_delivered[grpname] += 1

					found += 1


	# GM recv thread receives messages that the group manager sends.
	# That includes view changes and PINGs
	def _run_gm_recv(self):
		global FAULT
		global LEAVE
		
		while True:
			# Lock the tcp socket
			self._tcp_mtx.acquire()

			# Get the message type and do something
			# depending on it
			msg_type = utils.myrecv(self._gm_sock, 1)
			msg_type = struct.unpack("!B", msg_type)[0]
			if msg_type == gcp.PING:
				# Send pong
				pong = struct.pack("!B", gcp.PONG)
				self._gm_sock.sendall(pong)
			elif msg_type == gcp.JOIN_VC:
				self._handle_join_vc()
			elif msg_type == gcp.LEAVE_VC:
				self._handle_rmv_vc(LEAVE)
			elif msg_type == gcp.FAULT_VC:
				self._handle_rmv_vc(FAULT)

			# Unlock the tcp socket
			self._tcp_mtx.release()

			# Be nice
			time.sleep(0.01)

	# Handles a removal (leaver or fault) view
	# change. rmv_type = FAULT || LEAVE
	def _handle_rmv_vc(self, rmv_type):
		# Get removed member internet address
		rmv_ip = utils.myrecv(self._gm_sock, 4)
		rmv_ip = socket.inet_ntop(socket.AF_INET, rmv_ip)
		rmv_port = utils.myrecv(self._gm_sock, 2)
		rmv_port = struct.unpack("!H", rmv_port)[0]

		# Get groupname
		grp_len = utils.myrecv(self._gm_sock, 1)
		grp_len = struct.unpack("!B", grp_len)[0]
		grpname = utils.myrecv(self._gm_sock, grp_len)
		grpname = struct.unpack("!" + str(grp_len) + "s", grpname)[0].decode()

		# Don't run concurrently with message handling
		self._msg_hand_mtx.acquire()

		# Find the member with addr = (rmv_ip, rmv_port) in
		# the view of grpname, then remove it from the view
		for mem in self._views[grpname][:]:
			if mem[0] == (rmv_ip, rmv_port):
				# Get the id for the announcement
				# before removing it
				rmv_id = mem[1]

				# Remove the view entry 
				self._views[grpname].remove(mem)

				break

		# ...and the expected seqno for that process in grpname
		self._delivered[grpname].pop(mem[0], None)

		# Remove all out of FIFO order messages sent in this group
		# by the removed process
		for msg in self._mbuf[grpname][:]:
			if msg[1] == (rmv_ip, rmv_port):
				self._mbuf[grpname].remove(msg)

		#print("Delivered: {}".format(self._delivered))
		#print("Mbuf: {}".format(self._mbuf))

		# If you are the first in the new view, when you weren't before
		# you became the new coordinator
		if self._views[grpname][0][0] == self._my_addr and not self._am_coord[grpname]:
			# Magic hypothesis: Messages sent by the faulty coordinator
			# will arrive before the removal view change
			self._total_seqno[grpname] = self._total_delivered[grpname]
			self._am_coord[grpname] = True

			print("I am the coordinator!")

			# Assign a total sequence number to all unumbered totally ordered messages
			# that arrived between the previous coordinator's crash and
			# you noticing that you have become the coordinator.
			# Those are all the messages in the total buffer without
			# a tseqnum (according to the magic hypothesis, all tseq
			# sent by the old coordinator have already arrived)
			for tmsg in self._total_buf[grpname]:
				if tmsg[5] is None:
					ip_int = struct.unpack("!I", socket.inet_aton(tmsg[1][0]))[0]
					tseq = struct.pack("!BHIIHB" + str(grp_len) + "sI",
									    gcp.TSEQ,
									    tmsg[2],
									    tmsg[0],
									    ip_int,
									    tmsg[1][1],
									    grp_len,
									    grpname.encode(),
									    self._total_seqno[grpname])
					self._total_seqno[grpname] += 1
					self._basic_multicast(tseq, grpname)

		self._msg_hand_mtx.release()

		#print("Views: {}".format(self._views))

		# Insert a <rmv_type> announcement in the recv buffer
		# for the application to get
		self._rbuf_not_empty.acquire()
		self._rbuf[self._grp_to_gsock[grpname]].append((rmv_type, rmv_id.decode()))

		# Notify someone blocked in grp_recv
		self._rbuf_not_empty.notifyAll()
		self._rbuf_not_empty.release()


	# Handles a join view change
	def _handle_join_vc(self):
		global JOIN

		# Get joined member id
		id_len = utils.myrecv(self._gm_sock, 1)
		id_len = struct.unpack("!B", id_len)[0]
		join_id = utils.myrecv(self._gm_sock, id_len)
		join_id = struct.unpack("!" + str(id_len) + "s", join_id)[0]

		# Get joined member address
		join_ip = utils.myrecv(self._gm_sock, 4)
		join_ip = socket.inet_ntop(socket.AF_INET, join_ip)

		# Then get port
		join_port = utils.myrecv(self._gm_sock, 2)
		join_port = struct.unpack("!H", join_port)[0]

		# Get the name of the group that the vc concerns
		grp_len = utils.myrecv(self._gm_sock, 1)
		grp_len = struct.unpack("!B", grp_len)[0]
		grpname = utils.myrecv(self._gm_sock, grp_len)
		grpname = struct.unpack("!" + str(grp_len) + "s", grpname)[0].decode()

		# Send the current sequence number to the newly joined
		# member as the first seq num for which it will have to enforce FIFO
		# order
		init_seq = struct.pack("!BIB" + str(grp_len) + "s", 
								gcp.INIT_SEQ, 
								self._seqno[grpname],
								grp_len,
								grpname.encode())
		self._rmp.send((join_ip, join_port), init_seq, len(init_seq))

		if self._am_coord[grpname]:
			# If you are the coordinator, also send the current total sequence
			# number to the new member
			init_tseq = struct.pack("!BIB" + str(grp_len) + "s",
				                    gcp.INIT_TSEQ,
				                    self._total_seqno[grpname],
				                    grp_len,
				                    grpname.encode())
			self._rmp.send((join_ip, join_port), init_tseq, len(init_tseq))

		# Add the member to that group's view
		self._views[grpname].append(((join_ip, join_port), join_id))

		# Also set the initial sequence number to 0 (as this member
		# joined after you, and check if some msg in the mbuf is in
		# order now) -> a message might have been received between
		# views.append() and delivered = 0
		self._msg_hand_mtx.acquire()
		self._delivered[grpname][(join_ip, join_port)] = 0
		self._check_buffer(grpname)
		self._msg_hand_mtx.release()

		#print("Views: {}".format(self._views))

		# Insert a join announcement in the recv buffer
		# for the application to get
		self._rbuf_not_empty.acquire()
		self._rbuf[self._grp_to_gsock[grpname]].append((JOIN, join_id.decode()))

		# Notify someone blocked in grp_recv
		self._rbuf_not_empty.notifyAll()
		self._rbuf_not_empty.release()


	# Waits in a loop while messages of type other than
	# req_type arrive and answers them
	def _skip_to_msgtype(self, req_type):
		global FAULT
		global LEAVE

		while True:
			msg_type = utils.myrecv(self._gm_sock, 1)
			msg_type = struct.unpack("!B", msg_type)[0]

			# If the requested message type arrived
			# return
			if msg_type == req_type:
				return

			# If a PING was sent, reply with a PONG
			if msg_type == gcp.PING:
				pong = struct.pack("!B", gcp.PONG)
				self._gm_sock.sendall(pong)
			elif msg_type == gcp.JOIN_VC:
				self._handle_join_vc()
			elif msg_type == gcp.LEAVE_VC:
				self._handle_rmv_vc(LEAVE)
			elif msg_type == gcp.FAULT_VC:
				self._handle_rmv_vc(FAULT)


	# Requested API

	# grp_join(): Returns gsock, init_view
	# The returned init view is of the form
	# [id1, id2, id3, id4, ...]
	def grp_join(self, grpname, myid):
		# Lock the tcp socket
		self._tcp_mtx.acquire()
		self._msg_hand_mtx.acquire()

		# Serialize join request
		join_req = struct.pack("!BB" + str(len(grpname)) + "sB" + str(len(myid)) + "sH",
			                   gcp.JOIN_REQ, 
			                   len(grpname), 
			                   grpname.encode(), 
			                   len(myid), 
			                   myid.encode(),
			                   self._rmp_port)

		# Send join request to GM
		self._gm_sock.sendall(join_req)

		# Wait for join reply, ignoring pings
		self._skip_to_msgtype(gcp.JOIN_REPLY)

		# Set the seqno you expect from yourself to 0
		self._delivered[grpname][self._my_addr] = 0

		# Get number of group members in view
		gmems = utils.myrecv(self._gm_sock, 2)
		gmems = struct.unpack("!H", gmems)[0]

		# If you are the only member in the group upon joining
		# (meaning that you were the first ever member of the
		# group), become coordinator
		if gmems == 1:
			self._am_coord[grpname] = True
			self._total_seqno[grpname] = 0
			self._total_delivered[grpname] = 0
			print("I am the coordinator")
		
		# Initialize id list to be returned to app
		# to []
		grp_ids = []

		# Initialize the view for grpname, by
		# receiving all the member information that
		# the GM sent (in JOIN_MSTATE msgs) and inserting it.
		for k in range(0, gmems):
			self._skip_to_msgtype(gcp.JOIN_MSTATE)

			# Get id len
			id_len = utils.myrecv(self._gm_sock, 1)
			id_len = struct.unpack("!B", id_len)[0]

			# Get id
			mem_id = utils.myrecv(self._gm_sock, id_len)
			mem_id = struct.unpack("!" + str(id_len) + "s", mem_id)[0]

			# Get IP
			mem_ip = utils.myrecv(self._gm_sock, 4)
			mem_ip = socket.inet_ntop(socket.AF_INET, mem_ip)

			# time.sleep(100)

			# Then get port
			mem_port = utils.myrecv(self._gm_sock, 2)
			mem_port = struct.unpack("!H", mem_port)[0]
			
			# Add member id to id list
			grp_ids.append(mem_id)

			# Add member to middleware view
			self._views[grpname].append(((mem_ip, mem_port), mem_id))

		# Associate gsock with the group and compute next gsock
		self._gsock_to_grp[self._next_gsock] = grpname
		self._grp_to_gsock[grpname] = self._next_gsock
		self._next_gsock += 1

		self._msg_hand_mtx.release()

		# Unlock the tcp socket
		self._tcp_mtx.release()

		# DEBUG
		#print("Views: {}".format(self._views))
		#print("Gsocks: {}".format(self._gsock_to_grp))

		# Return gsock and grp_ids to app
		return self._next_gsock - 1, list(map(lambda x: x.decode(), grp_ids))


	def grp_leave(self, gsock):
		# Get group name
		grpname = self._gsock_to_grp[gsock]

		# Serialize leave request
		leave_req = struct.pack("!BB" + str(len(grpname)) + "s",
			                    gcp.LEAVE_REQ,
			                    len(grpname),
			                    grpname.encode())

		# Lock the tcp socket
		self._tcp_mtx.acquire()

		# Send leave request to GM
		self._gm_sock.sendall(leave_req)

		# Unlock the tcp socket
		self._tcp_mtx.release()

		# Delete everything pertaining to the group named
		# grpname, that is the gsock_to_grp entry and the
		# corresponding view. Don't do it while a message is
		# being handled.
		self._msg_hand_mtx.acquire()
		self._gsock_to_grp.pop(gsock, None)
		self._views.pop(grpname, None)
		self._grp_to_gsock.pop(grpname, None)
		self._rbuf.pop(gsock, None)
		self._mids.pop(grpname, None)
		self._seqno.pop(grpname, None)
		self._mbuf.pop(grpname, None)
		self._delivered.pop(grpname, None)
		self._total_buf.pop(grpname, None)
		self._total_delivered.pop(grpname, None)
		self._total_seqno.pop(grpname, None)
		self._am_coord.pop(grpname, None)
		self._tseqs.pop(grpname, None)
		self._msg_hand_mtx.release()

		# DEBUG
		#print("Views: {}".format(self._views))
		#print("Gsocks: {}".format(self._gsock_to_grp))
		#print("Delivered: {}".format(self._delivered))
		#print("Mbuf: {}".format(self._mbuf))

	def grp_send(self, gsock, msg, total):
		# Use the appropriate app msg type
		if total:
			msg_type = gcp.TMSG
		else:
			msg_type = gcp.MSG

		# Get the group name
		grpname = self._gsock_to_grp[gsock]
		grp_len = len(grpname)

		# Prepare MSG packet
		msg_len = len(msg)
		my_ip_int = struct.unpack("!I", socket.inet_aton(self._my_addr[0]))[0]
		msgp = struct.pack("!BHIIHB" + str(grp_len) + "sH" + str(msg_len) + "s",
			               msg_type, 
			               gsock,
			               self._seqno[grpname], 
			               my_ip_int,
			               self._my_addr[1],
			               grp_len, 
			               grpname.encode(), 
			               msg_len, 
			               msg)

		# Update sequence number
		self._seqno[grpname] += 1

		# Basic multicast the MSG packet
		self._msg_hand_mtx.acquire()
		self._basic_multicast(msgp, grpname)
		self._msg_hand_mtx.release()

	# grp_recv(): Returns type, msg
	def grp_recv(self, gsock, block):
		# Lock is used to check the condition safely
		self._rbuf_not_empty.acquire()

		# If a message is available for gsock
		if self._rbuf[gsock]:
			self._rbuf_not_empty.release()
			return self._rbuf[gsock].pop(0)

		# Else if there isn't, but block = False (non-blocking)
		elif not block:
			self._rbuf_not_empty.release()
			raise NonBlockException()

		# Wait until a message is received
		while not self._rbuf[gsock]:
			self._rbuf_not_empty.wait()

		self._rbuf_not_empty.release()

		return self._rbuf[gsock].pop(0)

	def get_udp_total(self):
		return self._rmp.get_udp_total()

if __name__ == "__main__":
	try:
		gc = GC()
	

		print("Commands: ")
		print("(J)oin group")
		print("(L)eave group")
		print("(R)ecv msg/vc")
		print("(E)xit\n")

		while True:
			option = str(input())
			if option == "J":
				grpname = str(input("Groupname: "))
				uid = str(input("User id: "))
				gsock, mem_uids = gc.grp_join(grpname, uid)
				print("\nGroup {} (gsock = {}) has members: ".format(grpname, gsock))
				for m in mem_uids:
					print("> " + m)
			elif option == "L":
				gsock = int(input("Gsock: "))
				gc.grp_leave(gsock)
			elif option == "E":
				sys.exit(0)
	except DiscFailException:
		print("Failed to discover GM!")
	except ConnTimeoutException:
		print("Connection to GM timed out!")
