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
import traceback

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

    zmq_socket = context.socket(zmq.PULL)
    zmq_socket.bind("tcp://*:5591")

    # Receive all chunks and insert them into the symbols array
    symbols = []
    while(len(symbols) < max_erasures):
        try:
    #Setup listen to all 3 respons sockets
    #poller = zmq.Poller()
    #poller.register(response_socket1,zmq.POLLIN)
    #poller.register(response_socket2,zmq.POLLIN)
    #poller.register(response_socket3,zmq.POLLIN)
            #message = zmq_socket.recv_string(flags=zmq.NOBLOCK)
            #print("Received reply %s " % message)
            result = zmq_socket.recv_multipart()
            symbols.append({
                "chunkname": result[0].decode('utf-8'),
                "data": bytearray(result[1])
            })
        except zmq.ZMQError as e:
            if e.errno == zmq.EAGAIN:
                pass # no message was ready (yet!)
            else:
                traceback.print_exc()

    print("All coded fragments received successfully")

    #Reconstruct the original file data
    file_data = decode_file(symbols)

    return file_data[:file_size]

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
