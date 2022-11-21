import zmq
import logging
from uuid import uuid4
import sys
import os


data_folder = sys.argv[1] if len(sys.argv) > 1 else './'
port = sys.argv[2] if len(sys.argv) > 1 else '5560'


if data_folder != './':    
    try:
        os.mkdir(os.path.join('./', data_folder))
    except FileExistsError:
        pass

logging.info(f'Storage node: {storage_node_id} is using data folder: {data_folder}')

lead_send_address = 'tcp://localhost:5558'
lead_recv_address = 'tcp://localhost:5561'

context = zmq.Context()

# Create pull socket for receiving message from controller
reciver = context.socket(zmq.SUB)
reciver.connect(lead_recv_address)

# Create push socket for responding to controller
sender = context.socket(zmq.PUSH)
sender.connect(lead_send_address)

# Pair connection - to communicate with other storage nodes
pairsocket = context.socket(zmq.PAIR)
pairsocket.bind("tcp://*:%s" % port)

pairconnection = context.socket(zmq.PAIR)


# Send id to controller 
idTask = messages_pb2.node_init()
idTask.port = port

sender.send(idTask.SerializeToString())


# Using polling to listen to 2 sockets at once
poller = zmq.Poller()
poller.register(reciver, zmq.POLLIN)
poller.register(pairsocket, zmq.POLLIN)


while True:
    try:
        sockets = dict(poller.poll())
    except KeyboardInterrupt:
        break

    if reciver in sockets:
        msg = reciver.recv_multipart()
        task = messages_pb2.retrieve_data_request()

        task.ParseFromString(msg[0])
        
        filename = task.filename
        filename += '.bin'

        try:
            with open(os.path.join(data_folder, filename), 'rb') as f:
                sender.send_multipart([bytes(filename, 'utf-8'), f.read()])
        except FileNotFoundError:
            pass


    
    if pairsocket in sockets:
        
        msg = pairsocket.recv_multipart()
        task = messages_pb2.send_data_request()
        task.ParseFromString(msg[0])

        numReplica = task.num_rep - 1
        nodeList = task.node_list
        nodeList = nodeList.replace(port,'')
        fileName = task.filename

        data = msg[1]

        
        logging.info(f'Filename: {filename} Size: {len(data)}')

        with open(os.path.join('./', data_folder, filename), 'wb') as f:
            f.write(data)
        
        sender.send_string(f'{storage_node_id}: saved the file')
        
        # Remove self from list -> send to next storage node
        sendtask = messages_pb2.send_data_request()
        sendtask.num_rep = int(numReplica)
        sendtask.nodeList = str(nodeList)
        sendtask.filename = str(fileName)

        pairconnection.send_multipart([
            sendtask.SerializeToString(),
            data
        ])

        logging.info(f'Sending to next replica. Number of replicas left: {numReplica}')




    
