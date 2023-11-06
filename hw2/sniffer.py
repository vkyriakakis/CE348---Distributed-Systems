import gcom
import sys

# Small program made to test grp_recv with block = True
# it joins the group with name argv[1]
# and just prints the various messages that are tossed around.
sf = open(sys.argv[2], "w")

if __name__ == "__main__":
	GC = gcom.GC()
	gsock, _ = GC.grp_join(sys.argv[1], sys.argv[2])
	mtype = None
	while True:
		mtype, msg = GC.grp_recv(gsock, True)
		if mtype == gcom.JOIN:
			print("{} joined {}.".format(msg, sys.argv[1]))
		elif mtype == gcom.LEAVE:
			print("{} left {} voluntarily.".format(msg, sys.argv[1]))
		elif mtype == gcom.FAULT:
			print("{} left {} by crashing.".format(msg, sys.argv[1]))
		elif mtype == gcom.MSG:
			print("{}".format(msg[1].decode()), file = sf)
			sf.flush()
			print("{}".format(msg[1].decode()))
