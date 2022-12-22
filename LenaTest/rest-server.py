from flask import Flask, make_response, send_file, g, request
import sqlite3
import zmq
import time
import sys
import reedsolomonModified
import io
import logging
import os
import messages_pb2
from utils import is_raspberry_pi
import json
import random



f = open(os.path.join(sys.path[0], 'times.txt'), 'a+')

f.close()

# Number replicas to create
NUM_REPLICAS = 1
MAX_ERASURES = 2

# List with nodes to send to
node_address = ["1", "2", "3"]



def get_db():
    if "db" not in g:
        g.db = sqlite3.connect("files.db", detect_types=sqlite3.PARSE_DECLTYPES)
        g.db.row_factory = sqlite3.Row
    return g.db


def close_db(e=None):
    db = g.pop("db", None)

    if db is not None:
        db.close()

pull_address = 'tcp://*:5557'
push_address = 'tcp://*:5558'
subscriber_address = 'tcp://*:5559'
encoding_push_address = 'tcp://*:5560'


if is_raspberry_pi():
    server_address = input('Server address: 192.168.0.___')
    pull_address = f'tcp://192.168.0.{server_address}:5557'
    push_address = f'tcp://192.168.0.{server_address}:5558'
    subscriber_address = f'tcp://192.168.0.{server_address}:5559'
    encoding_push_address = f'tcp://192.168.0.{server_address}:5560'


context = zmq.Context()

# Socket to send tasks to Storage Nodes
send_task_socket = context.socket(zmq.PUSH)
send_task_socket.bind(pull_address)

# Socket to receive messages from Storage Nodes
response_socket = context.socket(zmq.PULL)
response_socket.bind(push_address)

# Publisher socket for data request broadcasts
data_req_socket = context.socket(zmq.PUB)
data_req_socket.bind(subscriber_address)

# Socket to send encoding task to 'random storage node'
encoding_socket = context.socket(zmq.PUB)
encoding_socket.bind(encoding_push_address)





# Wait for all workers to start and connect.
time.sleep(1)
print("Listening to ZMQ messages on tcp://*:5558")

app = Flask(__name__)
app.teardown_appcontext(close_db)


@app.route("/")
def ping():
    return make_response({"message": "pong"})


@app.route("/files", methods=["GET"])
def list_files():
    # Open db conn
    # Fetch all files
    pass


@app.route("/files/<int:file_id>", methods=["GET"])
def download_file(file_id):
    # Open db conn
    # Fetch file with given id
    
    db = get_db()
    cursor = db.execute('SELECT * FROM `file` WHERE `id`=?', [file_id])
    
    if not cursor:
        return make_response({'message' : 'Error connecting to the database'}, 500)
    
    res = cursor.fetchone()
    if not res:
        return make_response({'message' : f'File {file_id} not found!'}, 404)

    res = dict(res)
 

    if res['storage_mode'] == 'erasurecode_rs':
        print(res['storage_details'])
        storage_details = json.loads(res['storage_details'])
        print(type(storage_details))
        coded_fragments = storage_details['fragment_names']
        max_erasures = MAX_ERASURES

        header = messages_pb2.header()
        header.request_type = messages_pb2.MESSAGE_DECODE

        task = messages_pb2.getdataErasure_request()
        task.filename = str(file_id)
        task.max_erasures = max_erasures
        task.file_size = res['size']
        task.coded_fragments = json.dumps(coded_fragments)

        encoding_socket.send_multipart([
            random.choice(node_address).encode('UTF-8'),
            header.SerializeToString(),
            task.SerializeToString()]
        )

        file_data = response_socket.recv()

        return send_file(io.BytesIO(file_data),mimetype=res['content_type'])
    
    task = messages_pb2.getdata_request()
    task.filename = str(file_id)
    storage_details = json.loads(res['storage_details'])

    data_req_socket.send(task.SerializeToString())

    result = response_socket.recv_multipart()
    filename = result[0].decode('utf-8')

    # File data is in the 2nd frame of the message
    data = result[1]

    return send_file(io.BytesIO(data), mimetype=res['content_type'])


@app.route("/files/<int:file_id>", methods=["DELETE"])
def delete_file(file_id):
    # Delete from k storage nodes
    # Delete from db
    

    return 



@app.route("/files", methods=["POST"])
def add_files():
    start_time = time.time()

    # Get payload from request
    # Add to db
    # Make copies to k storage nodes

    files = request.files
    if not files or not files.get('file'):
        logging.error('No file was uploaded in the request!')
        return make_response('File missing!', 400)
   
    file = files.get('file')
    filename = file.filename
    content_type = file.mimetype
    
    #do we need to read the file and not just send it forward?
    data = bytearray(file.read())
    size = len(data)

    db = get_db()
    cursor = db.execute(
        """INSERT INTO file(filename, size, content_type)
           VALUES (?, ?, ?)""",
        (filename, size, content_type),
    )

    db.commit()

    task = messages_pb2.storedata_request()
    # using the db index as a unique index
    task.filename = str(cursor.lastrowid)


    for idx in range(NUM_REPLICAS):
        send_task_socket.send_multipart([
            task.SerializeToString(),
            data
        ])

    for idx in range(NUM_REPLICAS):
        resp = response_socket.recv_string()
        logging.info(f'{idx}: {resp}')

    end_time = time.time()

    time_lapsed = end_time - start_time

    f = open(os.path.join(sys.path[0], 'times.txt'), 'a+')

    f.write("Size: {0}, Time: {1}\n".format(size,time_lapsed))
    f.close()

    print(time_lapsed)

    return make_response({"id": cursor.lastrowid}, 201)

@app.route("/files/erasurecode_rs", methods=["POST"])
def add_files_rs():
    start_time = time.time()
    payload = request.form


    files = request.files
    if not files or not files.get('file'):
        logging.error('No file was uploaded in the request!')
        return make_response('File missing!', 400)
   
    file = files.get('file')
    filename = file.filename
    content_type = file.mimetype
    storage_mode = payload.get('storage')
    
    #do we need to read the file and not just send it forward?
    data = bytearray(file.read())
    size = len(data)

    if storage_mode == 'erasurecode_rs':

        max_erasures = int(payload.get('max_erasures', 1))
        print("Max erasures: %d" % (max_erasures))


        header = messages_pb2.header()
        header.request_type = messages_pb2.MESSAGE_ENCODE

        task = messages_pb2.storedata_request()
        task.filename = filename
        task.max_erasures = max_erasures

        print("Delegating erasure")
        # Send encoding task to one of the storage nodes
        encoding_socket.send_multipart([
            random.choice(node_address).encode('UTF-8'),
            header.SerializeToString(),
            task.SerializeToString(),
            data
        ])
        
        print("Awaiting Response")
        res = response_socket.recv_string()

        db = get_db()
        cursor = db.execute(
        "INSERT INTO file(filename, size, content_type, storage_mode, storage_details) VALUES (?,?,?,?,?)",
        (filename, size, content_type, storage_mode, res))
        db.commit()
        # Wait to get response from encoding storage node
        
        print("Saved file %s" %res)

        return make_response({"id": cursor.lastrowid}, 201)

    

       





if __name__ == "__main__":
    host_local_computer = "localhost"
    host_local_network = "0.0.0.0"
    app.run(host="localhost", port=9000)
