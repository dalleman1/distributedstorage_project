import json
import platform
import random
import zmq
import logging
import sys
import os
import time
import messages_pb2

data_folder = sys.argv[1] if len(sys.argv) > 1 else "./"
port = sys.argv[2] if len(sys.argv) > 1 else "5560"


if data_folder != "./":    
    try:
        os.mkdir(os.path.join("./", data_folder))
    except FileExistsError:
        pass

send_file_address = "tcp://localhost:5560"
reciver_ctrl_address = "tcp://localhost:5559"
sender_ctrl_address = "tcp://localhost:5558"
recv_file_req_address = "tcp://localhost:5557"

if platform.system() == "Linux":
    server_address = input("Server address: 192.168.0.___")
    send_file_address = f"tcp://192.168.0{server_address}::5560"
    reciver_ctrl_address = f"tcp://192.168.0.{server_address}:5559"
    sender_ctrl_address = f"tcp://192.168.0.{server_address}:5558"
    recv_file_req_address = f"tcp://192.168.0.{server_address}:5557"

context = zmq.Context()

# Create pull socket for receiving message from controller
reciver_ctrl = context.socket(zmq.PULL)
reciver_ctrl.connect(reciver_ctrl_address)

# Create push socket for responding to controller
sender_ctrl = context.socket(zmq.PUSH)
sender_ctrl.connect(sender_ctrl_address)

# Create sub socket for reciving file requests
recv_file_req = context.socket(zmq.SUB)
recv_file_req.connect(recv_file_req_address)
recv_file_req.setsockopt(zmq.SUBSCRIBE, b'')

send_file = context.socket(zmq.PUSH)
send_file.connect(send_file_address)

# Pair connection - to communicate with other storage nodes
pair_socket = context.socket(zmq.PAIR)
pair_socket.bind(f"tcp://*:{port}")

pair_connection = context.socket(zmq.PAIR)

# Send id to controller 
idTask = messages_pb2.node_init()
idTask.port = port
sender_ctrl.send(idTask.SerializeToString())

# Waiting for connections
time.sleep(.1)

# Using polling to listen to 2 sockets at once
poller = zmq.Poller()
poller.register(reciver_ctrl, zmq.POLLIN)
poller.register(recv_file_req, zmq.POLLIN)
poller.register(pair_socket, zmq.POLLIN)

def send_to_other_node(task, data):
    task.num_rep -= 1
    
    node_list = json.loads(task.node_list)
    node_list.remove(port)

    task.node_list = json.dumps(node_list)

    node_port = random.choice(node_list)

    if platform.system() == "Linux":
        pair_conn_addr= f"tcp://192.168.0.{server_address}:{node_port}"
    else:
        pair_conn_addr = f"tcp://localhost:{node_port}"

    pair_connection.connect(pair_conn_addr)

    # Send to random node from node_list 
    pair_connection.send_multipart([task.SerializeToString(), data])
    msg = pair_connection.recv_string()
    print(msg)
    pair_connection.disconnect(pair_conn_addr)

while True:
    try:
        sockets = dict(poller.poll())
    except KeyboardInterrupt:
        break

    # Lead-node sends req to get file
    if recv_file_req in sockets:
        msg = recv_file_req.recv()
        task = messages_pb2.retrieve_data_request()
        task.ParseFromString(msg)
        
        filename = task.filename.split(".")[0]
        filename += ".bin"

        # Checks if file exists
        if os.path.exists(os.path.join(data_folder, filename)):
            # Read file from local disk and send to lean-node
            try:
                with open(os.path.join(data_folder, filename), "rb") as f:
                    send_file.send_multipart([bytes(filename, 'utf-8'), f.read()])
            except FileNotFoundError:
                print(f"Unable to open file: {filename}")
        else:
            print(f"Node #{port} does not have the following file: {filename}")

    if reciver_ctrl in sockets:
        msg = reciver_ctrl.recv_multipart()
        task = messages_pb2.send_data_request()
        task.ParseFromString(msg[0])

        # Replace file extension with bin 
        filename = task.filename.split(".")[0]
        filename += ".bin"

        data = msg[1]

        # Save file on local disk
        with open(os.path.join('./', data_folder, filename), "wb") as f:
            f.write(data)

        # TODO: How to send ACK?
        # Clashes with init message
        # sender_ctrl.send_string(f"Node #{port} saved the following file: {filename}")

        # Send to another node
        if (task.num_rep - 1) > 0: 
            send_to_other_node(task, data)
        

    if pair_socket in sockets:
        msg = pair_socket.recv_multipart()
        task = messages_pb2.send_data_request()
        task.ParseFromString(msg[0])

        filename = task.filename.split(".")[0]
        filename += '.bin'

        data = msg[1]

        logging.info(f"Filename: {filename} Size: {len(data)}")

        with open(os.path.join("./", data_folder, filename), "wb") as f:
            f.write(data)

        pair_socket.send_string(f'File stored by node: {port}')

        if (task.num_rep - 1) > 0: 
            send_to_other_node(task, data)

        print(f"Sending to next replica. Number of replicas left: {task.num_rep}")
