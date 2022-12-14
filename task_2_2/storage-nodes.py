from urllib import response
import zmq
import messages_pb2

import sys
import os
import random
import string
import reedsolomon

from utils import write_file

id = sys.argv[1] if len(sys.argv) > 1 else 0
data_folder = sys.argv[1] if len(sys.argv) > 1 else "./"
if data_folder != "./":
    try:
        os.mkdir('./' + data_folder)
    except FileExistsError as _:
        pass

print("Data folder %s " % data_folder)

encoding_subscriber_address = "tcp://localhost:5560"
sender_ctrl_address = "tcp://localhost:5558"


context = zmq.Context()

# Socket to listening for incoming encoding tasks
encoding_subscriber = context.socket(zmq.SUB)
encoding_subscriber.connect(encoding_subscriber_address)
encoding_subscriber.setsockopt_string(zmq.SUBSCRIBE, id)

# Create push socket for responding to controller
sender_ctrl = context.socket(zmq.PUSH)
sender_ctrl.connect(sender_ctrl_address)


# Socket for Reed-Solomon - send out to other nodes
#rs_send_task_socket = context.socket(zmq.PUSH)
#rs_send_task_socket.bind(**INSERT address**)

# Socket to receive from other nodes
#rs_response_socket = context.socket(zmq.PULL)
#rs_response_socket.bind(**INSERT address**)

poller = zmq.Poller()
poller.register(encoding_subscriber, zmq.POLLIN)

while True:
    try:
        socks = dict(poller.poll())
    except KeyboardInterrupt:
        break
    pass

    if encoding_subscriber in socks:
        
        msg = encoding_subscriber.recv_multipart()

        header = messages_pb2.header()
        header.ParseFromString(msg[1])

        if header.request_type == messages_pb2.MESSAGE_ENCODE:
            task = messages_pb2.storedata_request()
            task.ParseFromString(msg[2])

            data = msg[3]
            size = len(data)
            print("Chunk to save: %s, size: %d bytes " % (task.filename, len(data)))
            print("Max erasures is: %d" % task.max_erasures)
            sender_ctrl.send_string(task.filename)

            # Making call to reedsolomon
            #fragment_names = reedsolomon.store_file(data, task.max_erasures, send_task_socket,response_socket)


