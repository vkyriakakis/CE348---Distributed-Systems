import socket
import random
import threading
import time
import struct
import collections
import select
import errno
import argparse
import gcp
import utils

class GM:
	# View change format
	# (JOIN, IP, port, id, grpname)
	# (LEAVE, IP, port, grpname)
	# (FAULT, IP, port, grpname)
	def __init__(self, mcast_ip = "239.255.0.4", mcast_port = 8916, gm_port = 7777,
		         commu_timeout = 20):
		# groups: Dictionary indexed by 
		# Initialize UDP socket for discovery
		self._disc_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

		# Bind it to the multicast port and 
		# the IP you want the process to discover
		self._disc_sock.bind(("", mcast_port))

		# Add socket to the multicast group
		# on all interfaces.
		mreq = struct.pack('4sl', socket.inet_aton(mcast_ip), socket.INADDR_ANY)
		self._disc_sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

		# Initialize TCP listener socket
		self._listen_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

		# Bind it to the specified (ip, port)
		self._listen_sock.bind(("", gm_port))

		# Enable GM to accept connections
		self._listen_sock.listen()

		# Set it to non-blocking (mysterous SO race condition)
		self._listen_sock.setblocking(False)

		# Dictionary of group views for every group serviced by the
		# group manager, indexed by the group name. Each group view is a list of 
		# (tcp_addr, id, udp_addr) tuples.
		self._views = collections.defaultdict(list)

		# For debug
		#self._views[b"debaggu"] = [(("192.127.23.23", 20323), b"NPC")] * 1000

		# Timeout waiting for a process to send anything before a probe is attempted
		# and after that the timeout to wait for anything until it is declared as
		# faulty.
		self._commu_timeout = commu_timeout

		# Set of processes that are still in a TCP connection with the GM
		# and must be monitored for faults
		self._processes = []

		# Dictionary indexed by addr, stores True if the
		# corresponding process is in the ping-wait state
		# else it stores false
		self._ping_wait = {}

		# Dictionary indexed by addr, stores the last communication
		# time for each process
		self._last_commu = {}

		# Dictionary indexed by addr, contains the TCP socket for
		# each process
		self._proc_sock = {}

		# Does the reverse
		self._sock_proc = {}

		# Set of processes that were detected to be faulty through
		# ping-pong.
		self._fault_set = []

		# Dictionary indexed by addr, contains the TCP mtx for each
		# process
		self._tcp_mtxs = {}

		# Create and start the discovery thread
		_disc_thr = threading.Thread(target = self._run_disc)
		_disc_thr.daemon = True
		_disc_thr.start()

		# Create and start the ping thread
		_ping_thr = threading.Thread(target = self._run_ping)
		_ping_thr.daemon = True
		_ping_thr.start()

	# Discover thread code, it receives Discovery Request
	# multicast msgs and sends unicast Discovery Replies.
	def _run_disc(self):
		print("Disc thread reporting in o7")
		while True:
			# Wait for message
			msg, addr = self._disc_sock.recvfrom(1024)

			# Check if it was a discovery request, if not drop it
			if struct.unpack_from("!B", msg)[0] == gcp.DISC_REQ:
				# Send the discovery reply
				disc_reply = struct.pack("!B", gcp.DISC_REPLY)
				self._disc_sock.sendto(disc_reply, addr)

				# DEBUG
				print("Disc thr: {}".format(addr))

	# Ping thread code, it checks if a process hasn't sent anything in
	# a while, and if so it moves it to the PING WAIT state by sending a PING.
	# If it doesn't send anything in a long time even after the PING
	# it is considered faulty.
	def _run_ping(self):
		print("Ping thread reporting for duty o7")
		while True:
			for p in self._processes[:]:
				if not self._ping_wait[p]:
					if time.time() - self._last_commu[p] >= self._commu_timeout:
						# Set mode to pingwait
						self._ping_wait[p] = True

						# Lock tcp mutex of p, sockets aren't thread safe
						self._tcp_mtxs[p].acquire()

						# Send ping
						ping = struct.pack("!B", gcp.PING)

						try:	
							self._proc_sock[p].sendall(ping)
						except socket.error:
							pass

						# Unlock it
						self._tcp_mtxs[p].release()

						# Be nice 
						time.sleep(0.01)

				# If timeout while in ping_wait, consider it
				# faulty by removing it from the
				# monitor set and adding it to the faulty set
				elif time.time() - self._last_commu[p] >= 2 * self._commu_timeout:
					self._processes.remove(p)
					self._fault_set.append(p)

	# Multicast a message to all the members
	# of group <grpname>. This multicast is
	# performed by sending the message to each member
	# seperately by using TCP.
	def _gm_mcast(self, msg, grpname):
		for mem in self._views[grpname]:
			# Lock the TCP socket mutex
			# of each member before you send to it
			self._tcp_mtxs[mem[0]].acquire()
			try:
				self._proc_sock[mem[0]].sendall(msg)
			except socket.error:
				pass

			self._tcp_mtxs[mem[0]].release()


	def _handle_fault(self, faulty_addr):
		print("{} faulty!".format(faulty_addr))

		# Delete all data pertaining to the process
		del self._ping_wait[faulty_addr]
		del self._last_commu[faulty_addr]
		faulty_sock = self._proc_sock[faulty_addr]
		del self._sock_proc[faulty_sock]
		del self._proc_sock[faulty_addr]
		self._fault_set.remove(faulty_addr)
		del self._tcp_mtxs[faulty_addr]

		# Remove socket from read set if in it
		self._read_set.remove(faulty_sock)

		# Remove the process from all views that contain it
		for grpname in list(self._views.keys()):
			for p in self._views[grpname][:]:
				if p[0] == faulty_addr:
					udp_addr = p[2]
					self._views[grpname].remove(p)

					# Also, if the view is now empty, delete all traces of
					# the group
					if not self._views[grpname]:
						del self._views[grpname]

					# Else, send the fault view change to all remaining
					# members
					else:
						fault_ip_int = struct.unpack("!I", socket.inet_aton(udp_addr[0]))[0]
						fault_vc = struct.pack("!BIHB" + str(len(grpname)) + "s",
						                   gcp.FAULT_VC, fault_ip_int, udp_addr[1],
						                   len(grpname), grpname)
						self._gm_mcast(fault_vc, grpname)

					break

		print("Views: {}".format(self._views))

	def _handle_leave_request(self, conn_sock, conn_addr):
		# Receive group name length
		grp_len = utils.myrecv(conn_sock, 1)
		grp_len = struct.unpack("!B", grp_len)[0]

		# Get the groupname
		grpname = utils.myrecv(conn_sock, grp_len)
		grpname = struct.unpack("!" + str(grp_len) + "s", grpname)[0]

		# DEBUG
		#print("{} left group {}!".format(conn_addr, grpname.decode()))

		# Delete the process with address conn_addr from
		# the view of the group grpname
		for gmem in self._views[grpname][:]:
			if gmem[0] == conn_addr:
				# Get the udp address before deleting
				udp_addr = gmem[2]
				self._views[grpname].remove(gmem)
				break

		# If the view is now empty, delete all traces of
		# the group
		if not self._views[grpname]:
			del self._views[grpname]
			
		# Else send the leave view change to all remaining
		# members
		else:
			leave_ip_int = struct.unpack("!I", socket.inet_aton(udp_addr[0]))[0]
			leave_vc = struct.pack("!BIHB" + str(grp_len) + "s",
				                   gcp.LEAVE_VC, leave_ip_int, udp_addr[1],
				                   grp_len, grpname)
			self._gm_mcast(leave_vc, grpname)

		print("Views: {}".format(self._views))
				

	def _handle_join_request(self, conn_sock, conn_addr):
		# Get the groupname length
		grp_len = utils.myrecv(conn_sock, 1)
		grp_len = struct.unpack("!B", grp_len)[0]

		# Get the groupname
		grpname = utils.myrecv(conn_sock, grp_len)
		grpname = struct.unpack("!" + str(grp_len) + "s", grpname)[0]

		# Get the id length
		id_len = utils.myrecv(conn_sock, 1)
		id_len = struct.unpack("!B", id_len)[0]

		# Get the id
		join_id = utils.myrecv(conn_sock, id_len)
		join_id = struct.unpack("!" + str(id_len) + "s", join_id)[0]

		# Get the udp port
		udp_port = utils.myrecv(conn_sock, 2)
		udp_port = struct.unpack("!H", udp_port)[0]

		# DEBUG
		#print("{} joined group {} as {}!".format(conn_addr, grpname.decode(), 
		#                                                join_id.decode()))
		# Before replying to the join request
		# create the join view change message
		join_ip_int = struct.unpack("!I", socket.inet_aton(conn_addr[0]))[0]
		join_vc = struct.pack("!BB" + str(id_len) + "sIHB" + str(grp_len) + "s",
							  gcp.JOIN_VC, id_len, join_id, join_ip_int, udp_port,
							  grp_len, grpname)

		# and send it to all previous members
		self._gm_mcast(join_vc, grpname)

		# Add the process to the view of the group <grpname>
		self._views[grpname].append((conn_addr, join_id, (conn_addr[0], udp_port)))

		print("Views: {}".format(self._views))

		# Finally, send the join reply to the process
		join_reply_header = struct.pack("!BH", gcp.JOIN_REPLY, 
			                                   len(self._views[grpname]))

		self._tcp_mtxs[conn_addr].acquire()
		conn_sock.sendall(join_reply_header)
		self._tcp_mtxs[conn_addr].release()

		# Then send JOIN_MSTATE for all members of the group
		# including the one who joined
		for mem in self._views[grpname]:
			ip_int = struct.unpack("!I", socket.inet_aton(mem[2][0]))[0]
			join_reply_mem = struct.pack("!BB" + str(len(mem[1])) + "sIH",
				                         gcp.JOIN_MSTATE,
				                         len(mem[1]), 
				                         mem[1],
				                         ip_int, 
				                         mem[2][1])

			self._tcp_mtxs[conn_addr].acquire()
			conn_sock.sendall(join_reply_mem)
			self._tcp_mtxs[conn_addr].release()

	def loop(self):
		# Initialzie the select read set
		self._read_set = [self._listen_sock]

		while True:
			# Select is used to block until a connection request or
			# a GM message is sent.
			read_ready, _, _ = select.select(self._read_set, [], [], self._commu_timeout)

			# Loop over the returned read_ready set
			for sock in read_ready:
				# If the socket is the listening socket
				# a connection request was recved
				if sock == self._listen_sock:
					# The connection attempt might have
					# aborted and accept will block, it is
					# non-blocking for that reason
					try:
						conn_sock, conn_addr = self._listen_sock.accept()

						# Add the process' socket
						# to the reading set, and associate the
						# process with the socket
						self._proc_sock[conn_addr] = conn_sock
						self._sock_proc[conn_sock] = conn_addr
						self._read_set.append(conn_sock)

						# Initialize ping_wait to false,
						# and last communication time to the current time
						self._ping_wait[conn_addr] = False
						self._last_commu[conn_addr] = time.time()

						# Create a mutex for the process' TCP socket
						self._tcp_mtxs[conn_addr] = threading.Lock()

						# Add to processes last else ping thread
						# will not have all the resources needed
						self._processes.append(conn_addr)

					# Catch the non-block exception
					except socket.error as serr:
						if serr in (errno.EAGAIN, errno.EWOULDBLOCK):
							pass

				# Else if one of the processes sent data
				# (or gracefully closed the TCP connection)
				else:
					# Get the address of the process
					addr = self._sock_proc[sock]

					try:
						# Receive the type of the data
						msg_type = utils.myrecv(sock, 1)
						msg_type = struct.unpack("!B", msg_type)[0]

						# Depending on the msg_type do sth
						if msg_type == gcp.JOIN_REQ:
							self._handle_join_request(sock, addr)
						elif msg_type == gcp.LEAVE_REQ:
							self._handle_leave_request(sock, addr)
						elif msg_type == gcp.PONG:
							pass

						# In any case, the process communicated
						# with the GM so update last commu time
						# and end PING WAIT
						self._ping_wait[addr] = False
						self._last_commu[addr] = time.time()

					# Ignore connection close exception,
					# still unlock the mutex though
					except utils.ConnClosedError:
						pass

					# Ignore connection reset exception
					except socket.error as serr:
						if serr == errno.ECONNRESET:
							print("conn reset")
							pass 

			# Check if there were faulty processes and handle them
			for f in self._fault_set[:]:
				self._handle_fault(f)

def parse_args():
	parser = argparse.ArgumentParser(description = 'Group manager for ECE348 hw2.')
	parser.add_argument('--ping-timeout', '-P',
		                type = float,
		                action = 'store',
		                default = 20.0,
		                help = 'Timeout interval after sending a ping' +
		                       ' for a process to be considered dead.' + 
		                       ' Default is 20.0 secs.')

	return parser.parse_args()

if __name__ == "__main__":
	args = parse_args()
	gm = GM(commu_timeout = args.ping_timeout)
	gm.loop()
