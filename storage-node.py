import zmq
import logging
from uuid import uuid4
import messages_pb2
from utils import is_raspberry_pi
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

pull_address = 'tcp://localhost:5557'
push_address = 'tcp://localhost:5558'
subscriber_address = 'tcp://localhost:5559'

if is_raspberry_pi():
    server_address = input('Server address: 192.168.0.___')
    pull_address = f'tcp://192.168.0.{server_address}:5557'
    push_address = f'tcp://192.168.0.{server_address}:5558'
    subscriber_address = f'tcp://192.168.0.{server_address}:5559'

context = zmq.Context()

# Create pull socket for receiving message from controller
reciver = context.socket(zmq.PULL)
reciver.connect(pull_address)

# Create push socket for responding to controller
sender = context.socket(zmq.PUSH)
sender.connect(push_address)

# Create subscription socket for GET file messages
subscriber = context.socket(zmq.SUB)
subscriber.connect(subscriber_address)
subscriber.setsockopt(zmq.SUBSCRIBE, b'')

# Using polling to listen to 2 sockets at once
poller = zmq.Poller()
poller.register(reciver, zmq.POLLIN)
poller.register(subscriber, zmq.POLLIN)

while True:
    try:
        sockets = dict(poller.poll())
    except KeyboardInterrupt:
        break
    
    if reciver in sockets:
        msg = reciver.recv_multipart()
        task = messages_pb2.storedata_request()
        task.ParseFromString(msg[0])
        
        filename = task.filename
        filename += '.bin'

        data = msg[1]

        logging.info(f'Filename: {filename} Size: {len(data)}')

        with open(os.path.join('./', data_folder, filename), 'wb') as f:
            f.write(data)

        sender.send_string(f'{storage_node_id}: saved the file')
    
    if subscriber in sockets:
        
        msg = subscriber.recv()

        task = messages_pb2.getdata_request()
        task.ParseFromString(msg)

        filename = task.filename
        filename += '.bin'

        try:
            with open(os.path.join(data_folder, filename), 'rb') as f:
                sender.send_multipart([bytes(filename, 'utf-8'), f.read()])
        except FileNotFoundError:
            pass