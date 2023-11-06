import gcom
import select
import time
import random
import argparse
import sys

# Returns the cur_grp
def handle_input(gcm, user_in, groups, cur_grp, total):
	# If the input was a !join <grp> <myid> command
	if user_in.startswith("!join"):
		words = user_in.split(" ")
		grpname = words[1]
		myid = words[2]

		# Now that you parsed the command, 
		# join the group
		gsock, init_mems = gcm.grp_join(grpname, myid)

		# Print gsock and init_mems
		print("{} (gsock = {}) has members: ".format(grpname, gsock))
		for mem in init_mems:
			print(mem)

		# Add new group to groups
		groups.append((grpname, gsock))

		# Return the cur_grp
		return (grpname, gsock)

	# Else if the input was !leave <gsock>
	elif user_in.startswith("!leave"):
		words = user_in.split(" ")
		gsock = int(words[1])

		# Now you can leave the group
		gcm.grp_leave(gsock)

		# Remove the group <gsock> from groups
		# and maybe switch to None if you were in this group
		for k in range(0, len(groups)):
			if groups[k][1] == gsock:
				left_grp = groups[k]
				del groups[k]
				break

		print("You left {}.".format(left_grp[0]))
		if left_grp == cur_grp:
			print("Current group set to None.")
			return None

		return cur_grp

	# Else if the input was !switchto <gsock>
	elif user_in.startswith("!switchto"):
		words = user_in.split(" ")
		gsock = int(words[1])

		# Find the group with that gsock
		for grp in groups:
			if grp[1] == gsock:
				print("Switched to {}.".format(grp[0]))
				return grp

		return cur_grp

	# Else if the input was !quit
	elif user_in == "!quit":
		sys.exit(0)

	# Else if the input is not blank, interpret it as a message to be sent
	elif user_in.rstrip() != '':
		time.sleep(random.uniform(0, 0.8))
		gcm.grp_send(cur_grp[1], user_in.encode(), total)
		return cur_grp
	
	return cur_grp

def parse_args():
	# Returns an args object which contains all the relevant arguments
	parser = argparse.ArgumentParser(description = "Simple chat program to showcase ECE348 hw2.")
	parser.add_argument('-V', '--verbose', 
		                action = 'store_true',
		                dest = 'is_verbose',
		                help = 'Prints messages that show implementation details.')
	parser.add_argument('-B', '--bm-delay', 
					    action = 'store', 
				        default = 0.0,
		                type = float,
		                dest = 'bm_delay',
		                help = "The delay between successive unicast send() calls in a BM." +
		                       " Default is 0.")
	parser.add_argument('-P', '--ploss', 
		                action = 'store',
		                default = 0.0,
		                type = int,
		                help = 'Probability of UDP packet loss. ' + 
		                       'If 0 is given, no packet loss occurs, whereas if ' + 
		                       'N is given, packet loss occurs with probability 1/N.' +
		                       ' Default is 0.')
	parser.add_argument('-T', '--total', 
		                action = 'store_true',
		                dest = 'is_total',
		                help = 'Enables total order multicast mode.')

	return parser.parse_args()

if __name__ == "__main__":
	# ANSI color codes
	RED = '\033[91m'
	RST = '\033[0m'

	# Parse arguments
	args = parse_args()

	try:
		# Initialize the group communication middleware
		GC = gcom.GC(ploss = args.ploss, bm_delay = args.bm_delay, is_verbose = args.is_verbose)
	except gcom.DiscFailException:
		print("Failed to discover GM!")
		sys.exit(1)
	except gcom.ConnTimeoutException:
		print("Connection to GM timed out!")
		sys.exit(1)

	# Main loop, try to read input from user, and if they take too
	# long check any recieved messages via grp_recv()
	read_set = [sys.stdin]
	groups = [] # List of (grpname, gsock)
	cur_group = None
	while True:
		read_ready, _, _ = select.select(read_set, [], [], 1)
		if sys.stdin in read_ready:
			try:	
				user_in = input()
				cur_group = handle_input(GC, user_in, groups, cur_group, args.is_total)
			except EOFError:
				pass

		# Check current group (if it isn't None) for received messages
		try:
			if cur_group is not None:
				while True:
					mtype, data = GC.grp_recv(cur_group[1], False) # Don't block
					if mtype == gcom.JOIN:
						print(RED + "{} joined {}.".format(data, cur_group[0]) + RST)
					elif mtype == gcom.LEAVE:
						print(RED + "{} left {} voluntarily.".format(data, cur_group[0]) + RST)
					elif mtype == gcom.FAULT:
						print(RED + "{} left {} by crashing.".format(data, cur_group[0]) + RST)
					elif mtype == gcom.MSG:
						print(RED + "[{}]: {}".format(data[0], data[1].decode()) + RST)
		except gcom.NonBlockException:
			pass
