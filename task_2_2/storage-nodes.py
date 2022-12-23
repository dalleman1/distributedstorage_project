from urllib import response
import zmq
import messages_pb2
import json
import sys
import os
import random
import string
import reedsolomonModified

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

encoding_subscriber_address = "tcp://localhost:5560"
sender_ctrl_address = "tcp://localhost:5558"
Sender_recieve_address = "tcp://localhost:5557"
subscriber_address = "tcp://localhost:5559"


OwnPushAdd = f'556{id}'
OtherAdd_list = ['5561','5562','5563','5564']
OtherAdd_list.remove(OwnPushAdd)   #Make list of other add

context = zmq.Context()

Pull_RS_Socket_1 = context.socket(zmq.PULL) # Socket to receive from other nodes
Pull_RS_Socket_2 = context.socket(zmq.PULL) # Socket to receive from other nodes
Pull_RS_Socket_3 = context.socket(zmq.PULL) # Socket to receive from other nodes

Pull_Socket_List = [Pull_RS_Socket_1,Pull_RS_Socket_2,Pull_RS_Socket_3]

# Socket to listening for incoming encoding tasks
encoding_subscriber = context.socket(zmq.SUB)
encoding_subscriber.connect(encoding_subscriber_address)
encoding_subscriber.setsockopt_string(zmq.SUBSCRIBE, id)

# Socket to listening for incoming encoding tasks
decoding_subscriber = context.socket(zmq.SUB)
decoding_subscriber.connect(subscriber_address)
decoding_subscriber.setsockopt(zmq.SUBSCRIBE, b'')

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
poller.register(decoding_subscriber,zmq.POLLIN)
for element in range(0,2):
    Pull_Socket_List[element].connect(f'tcp://localhost:{OtherAdd_list[element]}')
    poller.register(Pull_Socket_List[element],zmq.POLLIN)

print("Setup done")

while True:
    try:
        socks = dict(poller.poll(100))
    except KeyboardInterrupt:
        break
    pass

    for socketNr in range(0,len(Pull_Socket_List)):
        if Pull_Socket_List[socketNr] in socks:
            print("recieved msg from rs_send_task_socket")
            msg = Pull_Socket_List[socketNr].recv_multipart()

            header = messages_pb2.header()
            header.ParseFromString(msg[0])

            if header.request_type == messages_pb2.MESSAGE_ENCODE:
                print(msg[2])
                task = messages_pb2.storedata_request()
                task.ParseFromString(msg[1])
                
                filename = task.filename
                filename += '.bin'

                data = msg[2]

                #logging.info(f'Filename: {filename} Size: {len(data)}')

                with open(os.path.join('./', data_folder, filename), 'wb') as f:
                    f.write(data)

                print(f'{id}: saved the file')
            elif header.request_type == messages_pb2.MESSAGE_DECODE:
                print("Recieved decode request from other node")
                task = messages_pb2.getdata_request()
                task.ParseFromString(msg[1])

                print("Finished parsing from string")
                filename = task.filename
                filename += '.bin'

                header = messages_pb2.header()
                header.request_type = messages_pb2.MESSAGE_DECODE
                
                try:
                    with open(os.path.join(data_folder, filename), 'rb') as f:
                        rs_push_socket.send_multipart([
                            header.SerializeToString(),
                            bytes(filename, 'utf-8'), 
                            f.read()])
                        print("Sending respons")
                except FileNotFoundError:
                    print("File not found with name:" + filename )
                    pass

    if Sender_recieve_socket in socks:
        print("Recieved msg on sender recieve")
        msg = Sender_recieve_socket.recv()

        task = messages_pb2.getdata_request()
        task.ParseFromString(msg)

        filename = task.filename
        filename += '.bin'
        print(filename)
        try:
            with open(os.path.join(data_folder, filename), 'rb') as f:
                sender_ctrl.send_multipart([bytes(filename, 'utf-8'), f.read()])
        except FileNotFoundError:
            pass

    if decoding_subscriber in socks:
        print("Recieved msg pn sender recieve")
        msg = decoding_subscriber.recv()

        task = messages_pb2.getdata_request()
        task.ParseFromString(msg)

        filename = task.filename
        filename += '.bin'

        try:
            with open(os.path.join(data_folder, filename), 'rb') as f:
                sender_ctrl.send_multipart([bytes(filename, 'utf-8'), f.read()])
        except FileNotFoundError:
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
            file = reedsolomonModified.get_file(
                json.loads(task.coded_fragments),
                task.max_erasures,
                task.file_size,
                rs_push_socket, 
                Pull_RS_Socket_1,
                Pull_RS_Socket_2,
                Pull_RS_Socket_3
                )

            sender_ctrl.send(file)


        if header.request_type == messages_pb2.MESSAGE_ENCODE:
            print("recieved msg from encoding_subscriber")
            task = messages_pb2.storedata_request()
            task.ParseFromString(msg[2])

            data = bytearray(msg[3])
            print("Storing")
            respons_filename = ""
            respons_data = bytearray()
            [fragment_names, respons_filename,respons_data] = reedsolomonModified.store_file(data, MAX_ERASURES, 
                rs_push_socket,
                Pull_RS_Socket_1,
                Pull_RS_Socket_2,
                Pull_RS_Socket_3)
            storage_details = {
                "code_fragments": fragment_names,
                "max_erasures": MAX_ERASURES
            }


            respons_filename += '.bin'
            #logging.info(f'Filename: {filename} Size: {len(data)}')

            
            with open(os.path.join('./', data_folder, respons_filename), 'wb') as f:
                f.write(respons_data)

            print(f'{id}: saved the file')

            size = len(data)
            print("Chunk to save: %s, size: %d bytes " % (task.filename, len(data)))
            print("Max erasures is: %d" % task.max_erasures)

            print(fragment_names)


            sender_ctrl.send_string(
                json.dumps({'fragment_names': fragment_names})
                )

           

