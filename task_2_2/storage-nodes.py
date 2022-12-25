from urllib import response
import zmq
import messages_pb2
import json
import sys
import os
import random
import string
import reedsolomonModified
import time

from utils import write_file

MAX_ERASURES = 2

id = sys.argv[1] if len(sys.argv) > 1 else 0
print("ID: ", {id})
data_folder = sys.argv[1] if len(sys.argv) > 1 else "./"
if data_folder != "./":
    try:
        os.mkdir('./' + data_folder)
    except FileExistsError as _:
        pass

print("Data folder %s " % data_folder)

# Test socket
subtest_address = "tcp://localhost:5589"

context = zmq.Context()

# Socket to receive get chunk message from the controller
sub_test = context.socket(zmq.SUB)
sub_test.connect(subtest_address)
sub_test.setsockopt(zmq.SUBSCRIBE, b'')
# TEEEEST END ###################################


encoding_subscriber_address = "tcp://localhost:5560"
sender_ctrl_address = "tcp://localhost:5558"
Sender_recieve_address = "tcp://localhost:5557"


OwnPushAdd = f'556{id}'
OtherAdd_list = ['5561','5562','5563','5564']
OtherAdd_list.remove(OwnPushAdd)   #Make list of other add


Pull_RS_Socket_1 = context.socket(zmq.PULL) # Socket to receive from other nodes
Pull_RS_Socket_2 = context.socket(zmq.PULL) # Socket to receive from other nodes
Pull_RS_Socket_3 = context.socket(zmq.PULL) # Socket to receive from other nodes

Pull_Socket_List = [Pull_RS_Socket_1,Pull_RS_Socket_2,Pull_RS_Socket_3]

# Socket to listening for incoming encoding tasks
encoding_subscriber = context.socket(zmq.SUB)
encoding_subscriber.connect(encoding_subscriber_address)
encoding_subscriber.setsockopt_string(zmq.SUBSCRIBE, id)

# Create push socket for responding to controller
sender_ctrl = context.socket(zmq.PUSH)
sender_ctrl.connect(sender_ctrl_address)

#Create socket to recieve from 
Sender_recieve_socket = context.socket(zmq.PULL)
Sender_recieve_socket.connect(Sender_recieve_address)


rs_push_socket = context.socket(zmq.PUSH) # Socket for Reed-Solomon - send out to other nodes

#Bind own puplish
rs_push_socket.bind(f'tcp://*:556{id}')

#Listen on multible 
poller = zmq.Poller()
poller.register(encoding_subscriber, zmq.POLLIN)
poller.register(sub_test, zmq.POLLIN)
for element in range(0,2):
    Pull_Socket_List[element].connect(f'tcp://localhost:{OtherAdd_list[element]}')
    poller.register(Pull_Socket_List[element],zmq.POLLIN)


### TEST FUCKING SOCKET ####
testpush_socket = context.socket(zmq.PUSH)
testpush_socket.connect("tcp://localhost:5591")



print("Setup done")

while True:
    try:
        socks = dict(poller.poll(100))
    except KeyboardInterrupt:
        break
    pass

    if sub_test in socks:
        msg = sub_test.recv_multipart()
        task = messages_pb2.getdata_request()
        task.ParseFromString(msg[2])

        print("Finished parsing from string")
        filename = task.filename
        filename += '.bin'
        print("Trying to find %s" %filename)

        try:
            with open(os.path.join(data_folder, filename), 'rb') as f:
                #testpush_socket.send_string(filename)
                #print("Sending response")
                testpush_socket.send_multipart([
                    bytes(filename, 'utf-8'),
                    f.read()
                ])
                print("Sending response")

        except FileNotFoundError:
            print("File not found with name:" + filename)
            pass        

    if encoding_subscriber in socks:
        print("Recieved msg on encoding sub")
        msg = encoding_subscriber.recv_multipart()

        header = messages_pb2.header()
        header.ParseFromString(msg[1])

        if header.request_type == messages_pb2.MESSAGE_DECODE:
            print("recieved decode msg")
            task = messages_pb2.getdataErasure_request()
            task.ParseFromString(msg[2])
            print(json.loads(task.coded_fragments))
            file_data = reedsolomonModified.get_file(
                json.loads(task.coded_fragments),
                task.max_erasures,
                task.file_size,
                Pull_RS_Socket_1,
                Pull_RS_Socket_2,
                Pull_RS_Socket_3
                )

            sender_ctrl.send(file_data)


           

