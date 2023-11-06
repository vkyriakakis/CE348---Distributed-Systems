import mynfs_cli as mynfs
import sys
import time
import os
import argparse

def parse_args():
	"""Returns an args object which contains all the relevant arguments"""
	parser = argparse.ArgumentParser(description = "MyNFS server program.")
	parser.add_argument('--chunk', '-C',
		                action = 'store',
		                type = int,
		                required = True,
		                help = 'Size of read chunk (in bytes).')
	parser.add_argument('-F', '--files', 
		                action = 'store',
		                nargs = '+',
		                required = True,
		                help = 'List of filenames to be read from the NFS server.')

	return parser.parse_args()

if __name__ == "__main__":
	args = parse_args()

	mynfs_cli = mynfs.MyNFSClient("192.168.1.6", 7777)

	# Open all fds
	src_fds = []
	copy_fds = []

	for f in args.files:
		copy_fds.append(os.open(f, os.O_WRONLY | os.O_CREAT | os.O_TRUNC))
		src_fds.append(mynfs_cli.open(f, [mynfs.O_RDWR]))

	# For every file
	for k in range(0, len(args.files)):
		# Read all bytes of src_fd and write them to copy_fd
		while True:
			data = mynfs_cli.read(src_fds[k], args.chunk)
			if not data:
				break

			os.write(copy_fds[k], data)

		print(args.files[k] + " done!")

	# Close all fds
	for c in copy_fds:
		os.close(c)

	for s in src_fds:
		mynfs_cli.close(s)

	#print(mynfs_cli.metrics())