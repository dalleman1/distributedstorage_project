from flask import Flask, make_response, g, request
import sqlite3
import zmq
import time
import io
import logging
from uuid import uuid4
import messages_pb2

# Number replicas to create
NUM_REPLICAS = 1

def get_db():
    if "db" not in g:
        g.db = sqlite3.connect("files.db", detect_types=sqlite3.PARSE_DECLTYPES)
        g.db.row_factory = sqlite3.Row
    return g.db


def close_db(e=None):
    db = g.pop("db", None)

    if db is not None:
        db.close()


context = zmq.Context()

# Socket to send tasks to Storage Nodes
send_task_socket = context.socket(zmq.PUSH)
send_task_socket.bind("tcp://*:5557")

# Socket to receive messages from Storage Nodes
response_socket = context.socket(zmq.PULL)
response_socket.bind("tcp://*:5558")

# Publisher socket for data request broadcasts
data_req_socket = context.socket(zmq.PUB)
data_req_socket.bind("tcp://*:5559")

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

    task = messages_pb2.getdata_request()
    task.filename = str(file_id)

    data_req_socket.send(task.SerializeToString())

    result = response_socket.recv_multipart()
    filename = result[0].decode('utf-8')

    # File data is in the 2nd frame of the message
    data = result[1]

    # return make_response(io.BytesIO(data), mimetype=res['content_type'])
    return make_response({'Hello': res}, 200)


@app.route("/files/<int:file_id>", methods=["DELETE"])
def delete_file(file_id):
    # Delete from k storage nodes
    # Delete from db
    

    return 



@app.route("/files", methods=["POST"])
def add_files():
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

    return make_response({"id": cursor.lastrowid}, 201)



if __name__ == "__main__":
    host_local_computer = "localhost"
    host_local_network = "0.0.0.0"
    app.run(host=host_local_computer, port=9000)
