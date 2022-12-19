import json
import logging
import platform
import time
import zmq
import messages_pb2

NUM_REPLICA = 2

recv_server_address = "tcp://localhost:5556"
send_server_address = "tcp://localhost:5555"

if platform.system() == "Linux":
    server_address = input("Server address: 192.168.0.___")
    recv_server_address = f"tcp://192.168.0.{server_address}:5556"
    send_server_address = f"tcp://192.168.0.{server_address}:5555"
    recv_job_address = f"tcp://192.168.0.{server_address}:5560"
    send_job_address = f"tcp://192.168.0.{server_address}:5559"
    recv_nodes_address = f"tcp://192.168.0.{server_address}:5558"
    req_file_address = f"tcp://192.168.0.{server_address}:5557"


context = zmq.Context()
recv_job_socket = context.socket(zmq.PULL)
recv_job_socket.bind(recv_job_address)

send_job_socket = context.socket(zmq.PUSH)
send_job_socket.bind(send_job_address)

recv_nodes_socket = context.socket(zmq.PULL)
recv_nodes_socket.bind(recv_nodes_address)

req_file_socket = context.socket(zmq.PUB)
req_file_socket.bind(req_file_address)

recv_server = context.socket(zmq.PULL)
recv_server.connect(recv_server_address)

send_server = context.socket(zmq.PUSH)
send_server.connect(send_server_address)

# Waiting for connections
time.sleep(.1)

poller = zmq.Poller()
poller.register(recv_nodes_socket)
poller.register(recv_server)

nodes = []

while True:
    try: 
        sockes = dict(poller.poll())
    except KeyboardInterrupt:
        break

    # Node sends init message to lead node
    if recv_nodes_socket in sockes:
        msg = recv_nodes_socket.recv()
        task = messages_pb2.node_init()
        task.ParseFromString(msg)

        if task.port not in nodes:
            nodes.append(task.port)
            print(f'The following node was added: {task.port}')
            logging.info(f'The following node was added: {task.port}')
        else:
            logging.warning(f'The following port is already used: "{task.port}"')

    # Flask server sends message
    if recv_server in sockes:
        start_time = time.time()
        msg = recv_server.recv_multipart()
        task = messages_pb2.data_request()
        task.ParseFromString(msg[0])

        # Req to store a file on nodes
        if task.type == 'store':
            if len(nodes) < NUM_REPLICA:
                logging.error(f'The system cannot do {NUM_REPLICA} way replica only {len(nodes)} is avaliable')
                break
            
            store_request = messages_pb2.send_data_request()
            store_request.filename = task.filename
            store_request.num_rep = NUM_REPLICA
            store_request.node_list = json.dumps(nodes)

            send_job_socket.send_multipart([store_request.SerializeToString(), msg[1]])
            send_server.send_string(f'{task.filename} has been stored!')

            print(f'Store file job has been send: {store_request}')
            logging.info(f'Store file job has been send: {store_request}')
        
        # Req to retrive file from nodes
        if task.type == 'retrive':
            retrive_request = messages_pb2.retrieve_data_request()
            retrive_request.filename = task.filename
            req_file_socket.send(retrive_request.SerializeToString())

            # Wait for response from a single node
            logging.info(f'Waiting for file "{task.filename}"')
            msg = recv_job_socket.recv()

            # Forwards to server 
            send_server.send(msg)

        time_lapsed = time.time() - start_time

        with open('leadTime.txt', 'a') as f:
            f.write(f"Size: {len(msg)}, Time: {time_lapsed}\n")