from urllib import response
import zmq
import messages_pb2
import json
import sys
import os
import random
import string
import reedsolomonModified
import copy
import time
import kodo

from utils import write_file
from multiprocessing.pool import ThreadPool
from multiprocessing import Process
import threading
from time import sleep

# task executed in a worker process
def empty_queue():
    msg = sub_test.recv_multipart()
    task = messages_pb2.getdata_request()
    task.ParseFromString(msg[2])

    print("Finished parsing from string")
    filename = task.filename
    filename += '.bin'
    print("Trying to find %s" %filename)

    try:
        with open(os.path.join(data_folder, filename), 'rb') as f:
                testpush_socket.send_multipart([
                    bytes(filename, 'utf-8'),
                    f.read()
                ])
        print("Sending response")

    except FileNotFoundError:
        print("File not found with name: " + filename)
        pass

    return 0


def decode_file(symbols):
    """
    Decode a file using Reed Solomon decoder and the provided coded symbols.
    The number of symbols must be the same as STORAGE_NODES_NUM - max_erasures.

    :param symbols: coded symbols that contain both the coefficients and symbol data
    :return: the decoded file data
    """

    # Reconstruct the original data with a decoder
    symbols_num = len(symbols)
    symbol_size = len(symbols[0]['data']) - symbols_num #subtract the coefficients' size
    decoder = kodo.block.Decoder(kodo.FiniteField.binary8)
    decoder.configure(symbols_num, symbol_size)
    data_out = bytearray(decoder.block_bytes)
    decoder.set_symbols_storage(data_out)

    for symbol in symbols:
        # Separate the coefficients from the symbol data
        coefficients = symbol['data'][:symbols_num]
        symbol_data = symbol['data'][symbols_num:]
        # Feed it to the decoder
        decoder.decode_symbol(symbol_data, coefficients)

    # Make sure the decoder successfully reconstructed the file
    assert(decoder.is_complete())
    print("File decoded successfully")

    return data_out
#

def get_file(coded_fragments, max_erasures, file_size):
    """
    Implements retrieving a file that is stored with Reed Solomon erasure coding

    :param coded_fragments: Names of the coded fragments
    :param max_erasures: Max erasures setting that was used when storing the file
    :param file_size: The original data size. 
    :param data_req_socket: A ZMQ SUB socket to request chunks from the storage nodes
    :param response_socket: A ZMQ PULL socket where the storage nodes respond.
    :return: A list of the random generated chunk names, e.g. (c1,c2), (c3,c4)
    """

    subscriber_address = 'tcp://*:5589'

    context = zmq.Context()

    # Publisher socket for data request broadcast
    data_req_socket_rs = context.socket(zmq.PUB)
    data_req_socket_rs.bind(subscriber_address)


    time.sleep(1)
    
    # We need 4-max_erasures fragments to reconstruct the file, select this many 
    # by randomly removing 'max_erasures' elements from the given chunk names. 
    fragnames = copy.deepcopy(coded_fragments)
    for i in range(max_erasures):
        fragnames.remove(random.choice(fragnames))
    
    # Request the coded fragments in parallel
    for name in fragnames:
        task = messages_pb2.getdata_request()
        header = messages_pb2.header()
        header.request_type = messages_pb2.MESSAGE_DECODE
        task.filename = name
        print(name)
        data_req_socket_rs.send_multipart(
            [
                b"Hello",
                header.SerializeToString(),
                task.SerializeToString()
            ])

    zmq_socket = context.socket(zmq.PULL)
    zmq_socket.bind("tcp://*:5591")

    poller = zmq.Poller()
    poller.register(zmq_socket, zmq.POLLIN)

    # Receive all chunks and insert them into the symbols array
    symbols = []
    while(len(symbols) < max_erasures):
        socks = dict(poller.poll())

        if socks.get(zmq_socket) == zmq.POLLIN:
            print("Received polling")
            try:
                result = zmq_socket.recv_multipart(flags=zmq.NOBLOCK)
                symbols.append({
                "chunkname": result[0].decode('utf-8'),
                "data": bytearray(result[1])
                })
            except zmq.EAGAIN as e:
                pass
            empty_queue()
  

    print("All coded fragments received successfully")

    # Reconstruct the original file data

    file_data = decode_file(symbols)

    return file_data[:file_size]



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

subtest_address = "tcp://localhost:5589"

context = zmq.Context()

# socket to receive get chunk message from the controller
sub_test = context.socket(zmq.SUB)
sub_test.connect(subtest_address)
sub_test.setsockopt(zmq.SUBSCRIBE, b'')
# TEEEEST END ###############

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

#Bind own publish
rs_push_socket.bind(f'tcp://*:556{id}')

#Listen on multible 
poller = zmq.Poller()
poller.register(encoding_subscriber, zmq.POLLIN)
poller.register(sub_test,zmq.POLLIN)
for element in range(0,2):
    Pull_Socket_List[element].connect(f'tcp://localhost:{OtherAdd_list[element]}')
    poller.register(Pull_Socket_List[element],zmq.POLLIN)


### TESTER socket ###
testpush_socket = context.socket(zmq.PUSH)
testpush_socket.connect('tcp://localhost:5591')

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
                task = messages_pb2.storedata_request()
                task.ParseFromString(msg[1])
                
                filename = task.filename
                filename += '.bin'

                data = msg[2]

                with open(os.path.join('./', data_folder, filename), 'wb') as f:
                    f.write(data)

                print(f'{id}: saved the file')

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
                testpush_socket.send_multipart([
                    bytes(filename, 'utf-8'),
                    f.read()
                ])
                print("Sending response")

        except FileNotFoundError:
            print("File not found with name: " + filename)
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

            pool = ThreadPool(processes=1)

            async_result = pool.apply_async(get_file, args=(json.loads(task.coded_fragments), task.max_erasures, task.file_size,))
            pool.close()

            pool.join()
            
            file_data = async_result.get()
           
            print("Received file from decoding")
            sender_ctrl.send(file_data)


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



