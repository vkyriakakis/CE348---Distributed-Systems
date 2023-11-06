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

	mynfs_cli = mynfs.MyNFSClient("192.168.1.6", 7777, cacheblocks, blocksize, fresht)
	while True:
		try:
			src_fd = mynfs_cli.open(sys.argv[1], [mynfs.O_RDWR])
		except OSError as e:
			if e.errno != errno.ENFILE:
				raise e
			else:
				print("Server file limit reached!!!", file = sys.stderr)
				time.sleep(10)
		else:
			break
			
	loops = 0 

	# Read all bytes from src_fd and write them to stdout
	while True:
		data = mynfs_cli.read(src_fd, 1024)
		if not data:
			break

		bytes_read += len(data)

		os.write(1, data)
		loops += 1

		#time.sleep(0.1)
		#input()
		#print("HYES")

	mynfs_cli.close(src_fd)

	#print(mynfs_cli.metrics())