import struct

# All packets used by the GC protocol
# are of the form:
# [type][content]
#   1B    varlen
#
# > GM Discovery Request (type = 0):
#     [0]
#     1B
#
# > GM Discovery Reply (type = 1):
#     [1]
#     1B
#
# > Join Request (type = 2):
#     [2][grp_len][grpname][id_len][id][udp_port]
#     1B    1B        var     1B    var    2B
#
# > Leave Request (type = 3):
#     [3][group_len][grpname]
#     1B    1B         var
#
# > Ping (type = 4):
#     [4]
#     1B
#
# > Pong (type = 5):
#     [5]
#     1B
#
# > Join View Change (type = 6):
#     [6][id_len][id][ip][udp_port][group_len][grpname]
#     1B   1B    var  4B     2B        1B        var
#
# > Leave View Change (type = 7):
#     [7][left_IP][left_port][group_len][grpname]
#     1B    4B        2B         1B       var
#
# > Fault View Change (type = 8):
#     [8][fault_IP][fault_port][group_len][grpname]
#     1B    4B         2B          1B        var
#
# > Join Reply (type = 9):
#     [9][gmems]
#      1Β  2Β   
#
# > Join Member State (type = 10):
#     [10][id_len][id][IP][udp_port]
#      1B    1B   var  4B     2B
#
# > Application Message (type = 11)
#     [11][acc_num][seqno][orig_IP][orig_port][grp_len][grpname][msg_len][msg]
#      1B    2B      4B      4B        2B        1B       var      2B     var
#
# acc_num: num given to the sender for a given access to the group,
# used to differentiate between accesses
# seqno: Is used for FIFO, and concerns the group grpname
#
# orig_IP, orig_port are needed because due to the reliable multicast
# retransmissions the same message might be returned by rmp with a
# different send_addr.
#
# > Initial Sequence number (type = 12)
#      [12][init_seqno][grp_len][grpname]
#       1B     4B         1B       var
#
# > Total-ordered application message (type = 13)
#     [13][acc_num][seqno][orig_IP][orig_port][grp_len][grpname][msg_len][msg]
#      1B    2B      4B      4B        2B        1B       var      2B     var
#
# > Total sequence number update (type = 14)
#     [14][acc_num][seqno][orig_IP][orig_port][grp_len][grpname][total_seqno]
#      1B    2B      4B      4B        2B         1B      var        4B
#
# > Initial Total Sequence number (type = 15)
#     [15][init_total_seqno][grp_len][grpname]
#      1B         4B            1B      var
#
#
DISC_REQ = 0
DISC_REPLY = 1
JOIN_REQ = 2
LEAVE_REQ = 3
PING = 4
PONG = 5
JOIN_VC = 6
LEAVE_VC = 7
FAULT_VC = 8
JOIN_REPLY = 9
JOIN_MSTATE = 10
MSG = 11
INIT_SEQ = 12
TMSG = 13
TSEQ = 14
INIT_TSEQ = 15