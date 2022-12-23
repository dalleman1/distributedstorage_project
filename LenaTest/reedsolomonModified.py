from email import header
from urllib import response
import kodo
import math
import random
import copy
from utils import random_string
import messages_pb2
import json
import zmq
import time

STORAGE_NODES_NUM = 4

RS_CAUCHY_COEFFS = [
    bytearray([253, 126, 255, 127]),
    bytearray([126, 253, 127, 255]),
    bytearray([255, 127, 253, 126]),
    bytearray([127, 255, 126, 253])
]

def get_file(coded_fragments, max_erasures, file_size,
              response_socket1, response_socket2, response_socket3):
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

    # Publisher socket for data request broadcasts
    data_req_socket_rs = context.socket(zmq.PUB)
    data_req_socket_rs.bind(subscriber_address)

    time.sleep(1)
    
    # We need 4-max_erasures fragments to reconstruct the file, select this many 
    # by randomly removing 'max_erasures' elements from the given chunk names. 
    fragnames = copy.deepcopy(coded_fragments)
    for i in range(max_erasures-1):
        fragnames.remove(random.choice(fragnames))
    
    # Request the coded fragments in parallel
    for name in fragnames:
        task = messages_pb2.getdata_request()
        header = messages_pb2.header()
        header.request_type = messages_pb2.MESSAGE_DECODE
        task.filename = name
        print(name)
        data_req_socket_rs.send_multipart(
            [b"Hello",header.SerializeToString(),
            task.SerializeToString()]
            )

    # Receive all chunks and insert them into the symbols array
    symbols = []

    #Setup listen to all 3 respons sockets
    poller = zmq.Poller()
    poller.register(response_socket1,zmq.POLLIN)
    poller.register(response_socket2,zmq.POLLIN)
    poller.register(response_socket3,zmq.POLLIN)

    print("Waiting for respons")
    while(len(symbols) < max_erasures):
        socks = dict(poller.poll(100))

        if response_socket1 in socks:
            print("Recieved respons")
            result = response_socket1.recv_multipart()
            # In this case we don't care about the received name, just use the 
            # data from the second frame
            symbols.append({
                "chunkname": result[0].decode('utf-8'), 
                "data": bytearray(result[1])
            })

        if response_socket2 in socks:
            print("Recieved respons")
            result = response_socket2.recv_multipart()
            # In this case we don't care about the received name, just use the 
            # data from the second frame
            symbols.append({
                "chunkname": result[0].decode('utf-8'), 
                "data": bytearray(result[1])
            })

        if response_socket3 in socks:
            print("Recieved respons")
            result = response_socket3.recv_multipart()
            # In this case we don't care about the received name, just use the 
            # data from the second frame
            symbols.append({
                "chunkname": result[0].decode('utf-8'), 
                "data": bytearray(result[1])
            })
    
    print("All coded fragments received successfully")

    #Reconstruct the original file data
    file_data = decode_file(symbols)

    return file_data[:file_size]
