import rrcli
import nfsp
import struct
import os
import errno
import time
import cache

import sys

# File access mode flags
O_RDONLY = 0
O_WRONLY = 1
O_RDWR = 2

# Open time flags
O_TRUNC = 4
O_CREAT = 8
O_EXCL = 16

# Values for whence (seek)
SEEK_SET = 0
SEEK_CUR = 1
SEEK_END = 2

# Chunk size for reading and writing
CHUNK_SIZE = 1300

class MyNFSClient:
	def __init__(self, server_ip, server_port, cacheblocks = 4, blocksize = 1300, fresh_t = 1.0):
		"""Initialize RR communication with server and
		NFS client's data structures."""
		self._rr = rrcli.RRClient(server_ip, server_port)

		# Read-write position for each fd
		self._rwpos = {}

		# Access mode for each fd
		self._mode = {}

		# fd to (server file id, session num) associations
		self._fd_to_id = {}

		# Used file descriptors
		self._used_fds = []

		# Most recent file size reported by server by fd
		self._fsize = {}

		# fd to filename associations
		self._fd_to_fname = {}

		# LRU client cache of <cacheblocks> blocks, 
		# is used to store file blocks
		# so that they can be retrieved on a read, 
		# in order to decrease client-server interactions.
		# Blocks are keyed by (filename, block_num) and
		# contain (block_data, tmod, tfresh)
		self._cache = cache.Cache(cacheblocks)

		# Store block size 
		self._block_size = blocksize

		# Store freshness interval
		self._fresh_t = fresh_t

		# Average req-reply time metric
		self._avg_rr_time = 0.0

	def open(self, fname, flags):
		"""Communicates with MyNFS server in order to locate/create
		the file, and obtain the server-side file id. Returns a client-side fd."""

		# Check if the provided flag combination is valid,
		# and get access mode + open time flags 
		mode, otime_flags = self._check_flags(flags)

		# Send open request to server and receives file data
		try:
			fid, session_num, fsize = self._open_file(fname, otime_flags)
		except Exception as e:
			raise e

		# Choose next fd
		fd = self._choose_fd()

		# Initialize all data for the fd
		self._rwpos[fd] = 0
		self._fd_to_id[fd] = (fid, session_num)
		self._fd_to_fname[fd] = fname
		self._used_fds.append(fd)
		self._fsize[fd] = fsize
		self._mode[fd] = mode

		# Print data structures (DEBUG)
		#self._print_data()

		return fd

	def read(self, fd, count):
		# """Reads and returns up to <count> bytes from the file
		global O_WRONLY

		# Check if fd exists
		if fd not in self._used_fds:
			raise OSError(errno.EBADF, os.strerror(errno.EBADF), 
				          "Invalid file descriptor!")

		# Check if fd is open for reading
		elif self._mode[fd] == O_WRONLY:
			raise OSError(errno.EBADF, os.strerror(errno.EBADF),
				          "File descriptor not open for reading!")

		# The read data chunks
		total_data = []
		bytes_read = 0

		# Compute start and end block indices
		start_block = self._rwpos[fd] // self._block_size
		end_block = (self._rwpos[fd] + count - 1) // self._block_size
		#print(start_block, end_block)

		# For all blocks E [start_block, end_block]
		for k in range(start_block, end_block + 1):
			#print(k, file = sys.stderr)
			# Look in the cache for the block
			block = self._cache.lookup((self._fd_to_fname[fd], k))
			if block is not None:
				# If the block was found in the cache
				# check its freshness
				if time.time() > block[2]:
					#print("HIT-NOT_FRESH")
					# The block wasn't fresh so do the validity check
					block_data = self._send_read_req(fd, k, block[0], block[1])
				else:
					# The block was fresh, so just take the data
					# from the cache
					#print("HIT-FRESH")
					block_data = block[0]
			else:
				# If the block was NOT found in the cache,
				# fetch it from the server
				#print("MISS")
				block_data = self._send_read_req(fd, k, None, -1)

			# If the server didn't have the block
			# stop reading
			if not block_data:
				#print("{} EOF".format(k))
				break

			# Get as much data as needed from the retrieved block
			start_pos = max(self._rwpos[fd] - k * self._block_size, 0)
			bytes_remaining = count - bytes_read
			needed_data = block_data[start_pos:start_pos + bytes_remaining]

			# Insert the data to the total
			total_data.append(needed_data)
			bytes_read += len(needed_data)

			# If the data received from the block is less than
			# the block_size, the block was the last, stop
			# asking the server for more
			if len(needed_data) < self._block_size:
				#print("EOB")
				break

			# Print cache
			#self._print_cache()

		#print(bytes_read)

		# Update read/write position
		self._rwpos[fd] += bytes_read

		#print(total_data)

		# Assemble the received bytes and return them
		return b"".join(total_data)

	def _send_read_req(self, fd, block_idx, block_data, tmod):
		""" Sends a read request (for validity check or plain
		fetch) and then returns the requested block (after
		updating the cache properly). A tmod == -1 indicates
		fetch mode, while a tmod != -1 means validity check."""

		fname = self._fd_to_fname[fd]

		# Do as many RR as needed with the server
		# so that the request is sent in the correct session
		while True:
			# If read request (validity ver.) must be sent
			if tmod != -1:
				read_req = struct.pack("!BII?IQH", 
					                   nfsp.READ,
					                   self._fd_to_id[fd][0],
					                   self._fd_to_id[fd][1],
					                   True,
					                   tmod,
					                   block_idx * self._block_size,
					                   self._block_size)
			# Else send read request (fetch ver.)
			else:
				read_req = struct.pack("!BII?QH", 
					                   nfsp.READ,
					                   self._fd_to_id[fd][0],
					                   self._fd_to_id[fd][1],
					                   False,
					                   block_idx * self._block_size,
					                   self._block_size)
			reply = self._rr.req_reply(read_req)

			# Depending on the reply's type, do sth
			rtype = struct.unpack_from("!B", reply)[0]
			#print(rtype)
			if rtype == nfsp.DATA:
				# If the type is data, receive the new block
				fsize, tmod, data_len = struct.unpack_from("!QIH", reply, 1)
				new_data = struct.unpack_from("!" + str(data_len) + "s", reply, 15)[0]
				self._fsize[fd] = fsize

				# Update the block in the cache (new mod, new data,
				# new tfresh)
				self._cache.insert((fname, block_idx),
					               (new_data, tmod, time.time() + self._fresh_t))
				#self._print_cache()
				block_data = new_data
				break
			elif rtype == nfsp.OK:
				# If type is OK, the block in the
				# cache is valid, so update fresh time
				self._cache.insert((fname, block_idx), 
					               (block_data, tmod, time.time() + self._fresh_t))
				#self._print_cache()
				break
			elif rtype == nfsp.ERR:
				# Raise an exception according to the error
				# code the server sent
				err_code = struct.unpack_from("!B", reply, 1)[0]
				raise OSError(err_code, os.strerror(err_code))
			elif rtype == nfsp.EOF:
				# If the server sent EOF, the block <block_idx> does
				# not exist after all in the file, so
				# stop trying to read blocks and remove the block
				# (or do nothing if you never inserted it)
				self._fsize[fd] = struct.unpack_from("!Q", reply, 1)[0]
				self._cache.remove((fname, block_idx))
				#self._print_cache()
				block_data = b''
				break	
			elif rtype == nfsp.BAD_SNUM:
				# The server session changed, the file must be reopended
				# to obtain a new file id
				fid, session_num, fsize = self._open_file(self._fd_to_fname[fd], 0)
				self._fd_to_id[fd] = (fid, session_num)
				self._fsize[fd] = fsize
				#self._print_data()

		return block_data

	def write(self, fd, data):
		"""Writes <data> to the file corresponding to fd."""
		global O_RDONLY

		# Check if fd exists
		if fd not in self._used_fds:
			raise OSError(errno.EBADF, os.strerror(errno.EBADF), 
				          "Invalid file descriptor!")
		# Check if fd is open for writing
		elif self._mode[fd] == O_RDONLY:
			raise OSError(errno.EBADF, os.strerror(errno.EBADF),
				          "File descriptor not open for writing!")

		data_len = len(data)

		# Compute number of chunks
		chunks = data_len // CHUNK_SIZE
		if data_len % CHUNK_SIZE:
			chunks += 1

		rwpos = self._rwpos[fd]

		# Write each chunks to the file in the server by
		# sending a write request for each. If the session in the
		# server changes, re-open the file to obtain a new id
		for k in range(0, chunks):
			chunk = data[k * CHUNK_SIZE:(k + 1) * CHUNK_SIZE]
			chunk_len = len(chunk)

			# Retry until there are no session changes
			while True:
				write_req = struct.pack("!BIIQH" + str(chunk_len) + "s",
					                    nfsp.WRITE, 
					                    self._fd_to_id[fd][0],
					                    self._fd_to_id[fd][1], 
					                    rwpos,
					                    chunk_len, 
					                    chunk)
				# Send request and get reply
				reply = self._rr.req_reply(write_req)

				# Act depending on the type of the reply
				rtype = struct.unpack_from("!B", reply)[0]
				if rtype == nfsp.OK:
					# If the reply is OK, move on to the next chunk
					self._fsize[fd], tmod = struct.unpack_from("!QI", reply, 1)

					# But first, pass the changes to any block
					# in the cache they cover
					self._write_to_cache(fd, chunk, tmod, rwpos)

					rwpos += chunk_len
					break
				elif rtype == nfsp.ERR:
					# If the reply is ERR, raise an exception
					# using the error code the server provided
					err_code = struct.unpack_from("!B", reply, 1)[0]
					raise OSError(err_code, os.strerror(err_code), self._fd_to_fname[fd])
				elif rtype == nfsp.BAD_SNUM:
					# The reply type was BAD_SNUM, so open the file again in
					# the new session
					fid, session_num, fsize = self._open_file(self._fd_to_fname[fd], 0)
					self._fd_to_id[fd] = (fid, session_num)
					self._fsize[fd] = fsize
					#self._print_data()

		self._rwpos[fd] = rwpos

		# All of the chunks were written,
		# return the number of written bytes
		# (always data_len)
		return data_len

	def _write_to_cache(self, fd, chunk, tmod, rwpos):
		""" Passes a write done to the server to all
		blocks in the cache it corresponds to. If a block
		doesn't exist in the cache it is NOT fetched (no-write allocate)."""

		fname = self._fd_to_fname[fd]

		# Compute start and end block indices
		chunk_len = len(chunk)
		start_block = rwpos // self._block_size
		end_block = (rwpos + chunk_len - 1) // self._block_size

		# Chunk position
		chunk_pos = 0

		#print("\nstart block: {}, end_block: {}".format(start_block, end_block))

		for k in range(start_block, end_block + 1):
			# Look for the block in the cache
			block = self._cache.lookup((fname, k))
			if block is not None:
				# If the block was found, write the data to it
				data_start = max(rwpos - k * self._block_size, 0)
				data_end = min(self._block_size, rwpos + chunk_len - k * self._block_size)
				new_data = block[0][:data_start] + \
				           chunk[chunk_pos:chunk_pos + data_end - data_start] + \
				           block[0][data_end:]

				#print("data start: {}, data end: {}, chunk pos: {}".format(data_start, 
				#	  data_end, chunk_pos))

				# Update chunk pos
				chunk_pos += data_end - data_start

				# Update the cache block in the cache
				#print(time.time())
				self._cache.insert((fname, k), 
					               (new_data, tmod, time.time() + self._fresh_t))
				#self._print_cache()
			else:
				# Don't write to cache, but still advance chunk_pos
				chunk_pos += self._block_size

	def seek(self, fd, pos, whence):
		""" Sets the read/write position for fd according to pos and
		whence."""
		global SEEK_SET
		global SEEK_CUR
		global SEEK_END

		# Check if fd is valid
		if fd not in self._used_fds:
			raise OSError(errno.EBADF, os.strerror(errno.EBADF), 
				          "Invalid file descriptor!")
		# Also check if the given whence is valid
		elif whence not in (SEEK_SET, SEEK_CUR, SEEK_END):
			raise OSError(errno.EINVAL, os.strerror(errno.EINVAL),
				          "Whence wasn't one of SEEK_SET, SEEK_CUR or SEEK_END!")

		# Compute the new rw position
		if whence == SEEK_SET:
			new_pos = pos
		elif whence == SEEK_CUR:
			new_pos = self._rwpos[fd] + pos
		else:
			new_pos = self._fsize[fd] + pos

		# If the new position is negative, raise exception
		if new_pos < 0:
			raise OSError(errno.EINVAL, os.strerror(errno.EINVAL),
				          "Given pos, whence gave negative offset!")

		# The new position is good, store it
		self._rwpos[fd] = new_pos

	def _open_file(self, fname, otime_flags):
		""" Sends an open request for <fname> to the
		server, and returns id, fsize, session_num. """
		fname_len = len(fname)
		open_req = struct.pack("!BBH" + str(fname_len) + "s",
			                   nfsp.OPEN, otime_flags, fname_len, fname.encode())

		# Send open request and wait for reply
		reply = self._rr.req_reply(open_req)

		# Handle the reply
		rtype = struct.unpack_from("!B", reply)[0]
		if rtype == nfsp.OK:
			# If the reply was OK, the server sent the id and session num
			fid, session_num, fsize = struct.unpack_from("!IIQ", reply,  1)
		elif rtype == nfsp.ERR:
			# If the reply was ERR, raise OSError with the sent error code
			err_code = struct.unpack_from("!B", reply, 1)[0]
			raise OSError(err_code, os.strerror(err_code), fname)

		print("{} -> {}, {}".format(fname, fid, session_num), file = sys.stderr)

		return fid, session_num, fsize

	def close(self, fd):
		""" Deletes all data for an fd, and frees the fd for future use."""

		# Check if the fd exists
		if fd not in self._used_fds:
			raise OSError(errno.EBADF, os.strerror(errno.EBADF), 
				          "Invalid file descriptor!")

		# Delete all of the fd's state
		del self._rwpos[fd]
		del self._fd_to_id[fd]
		del self._fd_to_fname[fd]
		del self._fsize[fd]
		del self._mode[fd]

		# Free for future use
		self._used_fds.remove(fd)

		# Print data structures (DEBUG)
		#self._print_data()

	def metrics(self):
		metrics = self._rr.metrics()
		return metrics[0], metrics[1], self._avg_rr_time / metrics[0]

	def _check_flags(self, flags):
		"""Checks if the flag combination <flags> is invalid, and if not
		it returns the provided access mode and the OR combination of
		O_TRUNC, O_EXCL, and O_CREAT flags."""

		global O_RDONLY
		global O_WRONLY
		global O_RDWR
		global O_CREAT
		global O_EXCL
		global O_TRUNC
		
		# Only one access mode flag should be specified
		if (O_RDONLY in flags and O_WRONLY in flags) or \
		   (O_RDONLY in flags and O_RDWR in flags) or \
		   (O_WRONLY in flags and O_RDWR in flags):
		   raise ValueError("More than one access mode specified!")

		# O_TRUNC should be specified only on a file open for writing
		elif O_TRUNC in flags and O_RDONLY in flags:
			raise ValueError("Included O_TRUNC without write mode!")

		# Compute otime_flags
		otime_flags = 0
		if O_TRUNC in flags:
			otime_flags |= O_TRUNC
		if O_CREAT in flags:
			otime_flags |= O_CREAT
		if O_EXCL in flags:
			otime_flags |= O_EXCL

		# Get the access mode (if there isn't one -> error)
		if O_RDONLY in flags:
			mode = O_RDONLY
		elif O_WRONLY in flags:
			mode = O_WRONLY
		elif O_RDWR in flags:
			mode = O_RDWR
		else:
			raise ValueError("No access mode was specified!")

		return mode, otime_flags

	def _choose_fd(self):
		"""Returns the next available file descriptor."""
		fd = 0
		while fd in self._used_fds:
			fd += 1

		return fd

	def _print_data(self):
		"""Prints all data structures for debugging purposes."""
		print('')
		print(self._rwpos)
		print(self._fd_to_id)
		print(self._fd_to_fname)
		print(self._used_fds)
		print(self._fsize)
		print(self._mode)

	def _print_cache(self):
		print(self._cache._block_data)