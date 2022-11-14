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

lead_send_address = 'tcp://localhost:5560'
lead_recv_address = 'tcp://localhost:5561'

context = zmq.Context()

# Create pull socket for receiving message from controller
reciver = context.socket(zmq.PULL)
reciver.connect(lead_recv_address)

# Create push socket for responding to controller
sender = context.socket(zmq.PUSH)
sender.connect(lead_send_address)

sender.send_string('helo')
