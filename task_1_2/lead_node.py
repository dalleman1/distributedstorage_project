import zmq
import logging
from uuid import uuid4
import sys
import os

storage_node_id = str(uuid4())
data_folder = sys.argv[1] if len(sys.argv) > 1 else './'

if data_folder != './':    
    try:
        os.mkdir(os.path.join('./', data_folder))
    except FileExistsError:
        pass

logging.info(f'Storage node: {storage_node_id} is using data folder: {data_folder}')

pull_address = 'tcp://*:5557'
push_address = 'tcp://*:5558'

lead_pull_address = 'tcp://*:5561'
lead_push_address = 'tcp://*:5560'

context = zmq.Context()

# Create pull socket for receiving message from controller
# reciver = context.socket(zmq.PULL)
# reciver.connect(pull_address)

# # Create push socket for responding to controller
# sender = context.socket(zmq.PUSH)
# sender.connect(push_address)

# Create socket for 
send_task_socket = context.socket(zmq.PUSH)
send_task_socket.bind(lead_pull_address)

recv_resp_socket = context.socket(zmq.PULL)
recv_resp_socket.bind(lead_push_address)

# Using polling to listen to 2 sockets at once
poller = zmq.Poller()
# poller.register(reciver, zmq.POLLIN)
# poller.register(recv_resp_socket, zmq.POLLIN)

workers = []

send_task_socket.send_string('Ping')

while True:
    msg = recv_resp_socket.recv()
    workers.append(msg)
    print(workers)
    # try:
    #     sockets = dict(poller.poll())
    # except KeyboardInterrupt:
    #     break

    # if reciver in sockets:
    #     pass

    # if recv_resp_socket in sockets:
    #     msg = recv_resp_socket.recv()
    #     workers.append(msg)
    #     print(workers)

                
