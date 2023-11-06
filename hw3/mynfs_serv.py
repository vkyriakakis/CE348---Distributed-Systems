import rrserv
import struct
import os
import sys
import nfsp
import errno
import argparse
import time

# Open time flags
O_TRUNC = 4
O_CREAT = 8
O_EXCL = 16

# ID timeout(in secs), all of the id related
# data for a file (especially the fd) is closed/deleted
# if the file hasn't been used for a while (so it is unlikely to be used
# soon). This is needed due to the limit, some ids have to be deleted
# so that new ids can be allocated.
ID_TIMEOUT = 60

def parse_args():
	"""Returns an args object which contains all the relevant arguments"""
	parser = argparse.ArgumentParser(description = "MyNFS server program.")
	parser.add_argument('root_name', 
		                action = 'store',
		                help = 'Name of root directory.')
	parser.add_argument('ip_addr', 
		                action = 'store',
		                help = 'IP address of MyNFS server.')
	parser.add_argument('-S', '--snum-file',
						action = 'store',
						type = str,
						default = "mynfs_session_num",
						dest = "snum_file",
						help = "Path of session number file.")
	parser.add_argument('-P', '--port', 
					    action = 'store', 
				        default = 7777,
		                type = int,
		                dest = 'port',
		                help = "UDP port used for MyNFS request-reply. Default port is 7777.")
	parser.add_argument('-L', '--limit',
		                action = 'store',
		                default = 10,
		                type = int,
		                dest = 'limit',
		                help = 'Limit of concurrently open files.')
	parser.add_argument('-V', '--verbose',
		                action = 'store_true',
		                dest = 'is_verbose',
		                help = 'Enable to print every noteworthy happening.')

	return parser.parse_args()

def get_session_num(snum_file):
	"""Returns the current session number."""

	# If the file already exists,
	# this isn't the first time the server starts
	# load, increment and write the next session number
	if os.path.exists(snum_file):
		fd = os.open(snum_file, os.O_RDWR)

		# Load session num
		session_num = os.read(fd, 4)
		session_num = struct.unpack("=I", session_num)[0]

		# Compute next session num and overwrite the old
		next_session_num = session_num + 1
		next_session_num = struct.pack("=I", next_session_num)
		os.lseek(fd, 0, os.SEEK_SET)
		os.write(fd, next_session_num)

		# Close the file
		os.close(fd)

	else:
		# If the file does not exist, create it
		# then initialize session num
		fd = os.open(snum_file, os.O_WRONLY | os.O_CREAT)

		session_num = 0

		# Write next session num to file
		next_session_num = struct.pack("=I", 1)
		os.write(fd, next_session_num)

		# Close the file
		os.close(fd)

	return session_num


def open_file(fname, otime_flags, fname_to_id, id_to_fd, next_fid, is_verbose):
	"""Tries to apply the open time flags sent by the
	client, and returns OK and the fd on success and <ERR, errno>
	on fail."""

	global O_TRUNC
	global O_CREAT
	global O_EXCL

	# Compute os.otime_flags from nfs.otime_flags
	os_flags = 0

	if otime_flags & O_TRUNC:
		os_flags |= os.O_TRUNC
	if otime_flags & O_CREAT:
		os_flags |= os.O_CREAT
	if otime_flags & O_EXCL:
		os_flags |= os.O_EXCL

	try:
		fd = os.open(fname, os_flags | os.O_RDWR)
		if fname not in fname_to_id:
			if is_verbose:
				print("fd created for {}".format(fname))
			id_to_fd[next_fid] = fd
		else:
			os.close(fd)
	except OSError as e:
		return nfsp.ERR, e.errno

	return nfsp.OK,

# Returns fid, next_id
def assign_fid(fname, fname_to_id, id_to_fname, next_fid, id_count):
	""" Find fid for fname if already assigned or assigns the next id
	to it."""

	# Check if a fid was already assigned to fname
	if fname in fname_to_id:
		# There was, so retrieve it
		fid = fname_to_id[fname]
	else:
		# There wasn't, so assign one now
		fname_to_id[fname] = next_fid
		fid = next_fid
		id_to_fname[next_fid] = fname

		# Compute the next id
		next_fid += 1

		# A new id was assigned, increase the count
		id_count += 1

	return fid, next_fid, id_count

def write_data(fd, count, data):
	""" "Safe" version of os.write() that writes
	*all* of the requested data. Also calls fsync() to write
	it immediately to the disk."""

	# Repeat the write call until everything is written
	bytes_written = 0
	while bytes_written < count:
		bytes_written += os.write(fd, data[bytes_written:])
	
	#print("{}/{}".format(bytes_written, count))

	# Flush to disk
	os.fsync(fd)

def read_data(fd, count):
	""" "Safe" version of os.read() that read as much data
	as possible <= count."""

	# Repeat the read call until everything is read
	bytes_read = 0
	total_data = []
	while bytes_read < count:
		data = os.read(fd, count - bytes_read)
		if not data:
			break

		total_data.append(data)
		bytes_read += len(data)

	return b"".join(total_data)

def clean_id_data(id_to_fname, fname_to_id, id_to_fd, id_timeout, id_count):
	""" For every currently existing file id
	 check if it has timed out, and if so, delete all of the related
	 data."""

	# Find the ids to be cleaned
	ids_to_clean = []
	for fid in id_timeout:
		if time.time() > id_timeout[fid]:
			ids_to_clean.append(fid)

	# Delete the date of the ids you found
	for fid in ids_to_clean:
		fname = id_to_fname[fid]
		del fname_to_id[fname]
		del id_to_fname[fid]
		os.close(id_to_fd[fid])
		del id_to_fd[fid]
		del id_timeout[fid]

		# One less fid in use
		id_count -= 1

	return id_count

def print_id_data(id_to_fname, fname_to_id, id_to_fd, id_timeout, id_count):
	""" Print all of the file id related data structures
	the server uses."""
	print(id_to_fname)
	print(fname_to_id)
	print(id_to_fd)
	print(id_timeout)
	print(id_count)

if __name__ == "__main__":
	# Get command line args
	args = parse_args()

	print("MyNFS server start up...")

	# Initialize request-reply server
	rr = rrserv.RRServer(args.ip_addr, args.port)

	# File id to file name associations (used for read, write)
	id_to_fname = {}

	# File name to file id association (used for open)
	fname_to_id = {}

	# File id to file descriptors
	id_to_fd = {}

	# Deletion timeout for every id,
	# if an id times out when an open request is to
	# be processed, all of its data is deleted and its file
	# descriptor closed.
	id_timeout = {}

	# Number of ids currently in use
	id_count = 0

	# Next file id to be assigned
	next_fid = 0

	# Get current session num
	session_num = get_session_num(args.snum_file)
	print("Session number is {}.".format(session_num))

	print("MyNFS server init finished.\n")

	# Server loop
	while True:
		request = rr.get_req()

		# Service client request depending on type
		rtype = struct.unpack_from("!B", request)[0]
		if rtype == nfsp.OPEN:
			# Do a pass over all in use fids, deleting any timed out fids
			# (must be done here, because the place where an empty id position
			# is needed is during the handling of an open request)
			id_count = clean_id_data(id_to_fname, fname_to_id, id_to_fd, id_timeout, id_count)
			#print_id_data(id_to_fname, fname_to_id, id_to_fd, id_timeout, id_count)

			# Unpack open request
			otime_flags, fname_len = struct.unpack_from("!BH", request, 1)
			fname = struct.unpack_from("!" + str(fname_len) + "s", request, 4)[0]

			# Append fname to root_name
			fname = args.root_name + "/" + fname.decode()

			if args.is_verbose:
				print("Open {}.".format(fname))

			# Check if there is space for one more id, if this
			# file isn't already open
			if id_count == args.limit and fname not in fname_to_id:
				# Despite the cleaning pass there wasn't any space,
				# so tell the requestor that no more files
				# can be opened (in the requestor's POV, the server
				# is out of fds) and skip the request
				reply = struct.pack("!BB", nfsp.ERR, errno.ENFILE)
				rr.send_reply(reply)
				continue

			# Try to apply open time flags
			app_ret = open_file(fname, otime_flags, fname_to_id, id_to_fd, next_fid, args.is_verbose)

			# If open_file() returned ERR, send an ERR reply to
			# the client with the error code.
			if app_ret[0] == nfsp.ERR:
				reply = struct.pack("!BB", nfsp.ERR, app_ret[1])
				rr.send_reply(reply)
				continue

			# Assign fid and compute next
			fid, next_fid, id_count = assign_fid(fname, fname_to_id, id_to_fname, next_fid, id_count)

			# print("\nId to Fname: {}".format(id_to_fname))
			# print("Fname to Id: {}".format(fname_to_id))
			# print("Id to fd: {}".format(id_to_fd))
			# print("next_fid: {}".format(next_fid))

			fsize = os.path.getsize(fname)

			# Form the OK reply and send it
			reply = struct.pack("!BIIQ", nfsp.OK, fid, session_num, fsize)
			rr.send_reply(reply)

			# Set the deletion timeout for this id
			id_timeout[fid] = time.time() + ID_TIMEOUT
			#print_id_data(id_to_fname, fname_to_id, id_to_fd, id_timeout, id_count)

		elif rtype == nfsp.READ:
			# Unpack the read request
			fid, req_snum, is_validity = struct.unpack_from("!II?", request, 1)

			# Check if the session num is yours,
			# if not, send a BAD_SNUM reply so that the
			# client will reopen the file. Now that ids can be deleted
			# also do this if you don't recognize the id
			
			if req_snum != session_num or fid not in id_timeout:
				reply = struct.pack("!B", nfsp.BAD_SNUM)
				rr.send_reply(reply)
				continue

			# A read request was made for a valid fid, so
			# it shouldn't be deleted too soon, update del timeout
			try:
				id_timeout[fid] = time.time() + ID_TIMEOUT
			except IndexError:
				#print(fid)
				sys.exit(1)

			# Get stats
			stats = os.stat(id_to_fd[fid])
			fsize = stats.st_size
			serv_tmod = int(stats.st_mtime)

			# Depending on whether this is a validity check request
			# the rest of the packet differs
			if is_validity:
				#print("VALIDITY ", end = "")
				tmod, pos, count = struct.unpack_from("!IQH", request, 10)

				if args.is_verbose:
					print("Read (valcheck) {}B at {}, {}".format(count, id_to_fname[fid], pos))

				# If the modification times are equal,
				# the block in the client's cache is valid, send an OK reply
				if tmod == serv_tmod:
					#print("OK")
					reply = struct.pack("!B", nfsp.OK)
					rr.send_reply(reply)
					continue
			else:		
				#print("FETCH ", end = "")
				pos, count = struct.unpack_from("!QH", request, 10)
				if args.is_verbose:
					print("Read (fetch) {}B at {}, {}".format(count, id_to_fname[fid], pos))

			# If the request was to fetch the block or the block
			# in the client's cache was invalid, try to read it as usual
			try:
				os.lseek(id_to_fd[fid], pos, os.SEEK_SET)
				data = read_data(id_to_fd[fid], count)
			except OSError as e:
				# Send ERR reply
				#print("err")
				reply = struct.pack("!BB", nfsp.ERR, e.errno)
				rr.send_reply(reply)
			else:
				if not data:
					#print("EOF")
					reply = struct.pack("!BQ", nfsp.EOF, fsize)
					rr.send_reply(reply)
				else:
					#print("DATA")
					data_len = len(data)
					reply = struct.pack("!BQIH" + str(data_len) + "s",
										nfsp.DATA, fsize, serv_tmod, data_len, data)
					rr.send_reply(reply)

		elif rtype == nfsp.WRITE:
			# Unpack the write request
			fid, req_snum, pos, data_len = struct.unpack_from("!IIQH", request, 1)
			data = struct.unpack_from("!" + str(data_len) + "s", request, 19)[0]

			# Check if the session num is the current, if not
			# send BAD_SNUM reply. Also do this if you
			# don't recognize the fid (they can be deleted now)
			if req_snum != session_num or fid not in id_timeout:
				reply = struct.pack("!B", nfsp.BAD_SNUM)
				rr.send_reply(reply)
				continue

			# A write request was made for the id, so don't delete it
			# very soon, update del timeout
			id_timeout[fid] = time.time() + ID_TIMEOUT

			if args.is_verbose:
				print("Write {}B to {}, {}!".format(data_len, id_to_fname[fid], pos))

			# Try to write the data, if an error happens send
			# errno to the client
			try:
				# Go to the specified position and write the data
				os.lseek(id_to_fd[fid], pos, os.SEEK_SET)
				write_data(id_to_fd[fid], data_len, data)
			except OSError as e:
				# Send ERR reply
				#print("ERR")
				reply = struct.pack("!BB", nfsp.ERR, e.errno)
				rr.send_reply(reply)
			else:
				# If everything went well, send an OK reply
				stats = os.fstat(id_to_fd[fid])
				fsize = stats.st_size
				tmod = int(stats.st_mtime)
				reply = struct.pack("!BQI", nfsp.OK, fsize, tmod)
				rr.send_reply(reply)