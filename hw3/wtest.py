import mynfs_cli as mynfs
import sys
import time
import os
import errno

if __name__ == "__main__":
	if len(sys.argv) < 5:
		print("Run me with <filename> <cacheblocks> <blocksize> <fresht>!")
		sys.exit(1)

	cacheblocks = int(sys.argv[2])
	blocksize = int(sys.argv[3])
	fresht = float(sys.argv[4])
	bytes_read = 0
	bytes_written = 0

	mynfs_cli = mynfs.MyNFSClient("192.168.1.6", 7777, cacheblocks, blocksize, fresht)
	while True:
		try:
			dest_fd = mynfs_cli.open(sys.argv[1], [mynfs.O_WRONLY, mynfs.O_CREAT, mynfs.O_TRUNC])
		except OSError as e:
			if e.errno != errno.ENFILE:
				raise e
			else:
				print("Server file limit reached!!!", file = sys.stderr)
				time.sleep(10)
		else:
			break

	# Read all bytes of stdin and write them to copy_fd
	while True:
		data = os.read(0, 1024)
		if not data:
			break
			
		bytes_read += len(data)

		# input()
		# print("HYES")

		bytes_written += mynfs_cli.write(dest_fd, data)

	mynfs_cli.close(dest_fd)

	#print(mynfs_cli.metrics())