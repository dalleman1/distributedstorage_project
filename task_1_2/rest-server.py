import io
import logging
import time
import zmq 
import messages_pb2
import sqlite3
import platform
from flask import Flask, make_response, send_file, g, request

# Helper functions
def get_db():
    if "db" not in g:
        g.db = sqlite3.connect("files.db", detect_types=sqlite3.PARSE_DECLTYPES)
        g.db.row_factory = sqlite3.Row
    return g.db

def close_db(e=None):
    db = g.pop("db", None)

    if db is not None:
        db.close()

def is_raspberry_pi():
    return platform.system() == 'Linux'


# Setup ZMQ
pull_address = 'tcp://*:5555'
push_address = 'tcp://*:5556'

if is_raspberry_pi():
    server_address = input('Server address: 192.168.0.___')
    pull_address = f'tcp://192.168.0.{server_address}:5555'
    push_address = f'tcp://192.168.0.{server_address}:5556'

context = zmq.Context()

# Socket to send tasks to Lead Node
send_lead_node = context.socket(zmq.PUSH)
send_lead_node.bind(push_address)

# Socket to receive messages from Lead Node
recv_lead_node = context.socket(zmq.PULL)
recv_lead_node.bind(pull_address)

# Wait for all workers to start and connect.
time.sleep(.1)

# Setup Flask
app = Flask(__name__)
app.teardown_appcontext(close_db)

@app.route('/files', methods=['POST'])
def add_files():
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
        """INSERT INTO file(filename, size, content_type) VALUES (?, ?, ?)""",
        (filename, size, content_type))
    
    db.commit()

    # Construct and send proto msg 
    task = messages_pb2.data_request()
    task.filename = str(cursor.lastrowid)
    task.type = 'store'

    # Send proto msg
    send_lead_node.send_multipart([
        task.SerializeToString(),
        data
    ])

    ack = recv_lead_node.recv()
    print(ack)
    return make_response({'id': cursor.lastrowid}, 201) 

@app.route('/files/<int:file_id>', methods=['GET'])
def download_file(file_id):
    db = get_db()
    cursor = db.execute('SELECT * FROM `file` WHERE `id`=?', [file_id])

    if not cursor:
        return make_response({'message': 'Error connecting to the database'}, 500)
    
    res = cursor.fetchone()
    if not res:
        return make_response({'message': f'File {file_id} not found!'}, 404)
    
    res = dict(res)

    # Construct and send proto msg
    task = messages_pb2.data_request()
    task.filename = str(file_id)
    task.type = 'retrive'
    
    # Send proto msg
    send_lead_node.send_multipart([task.SerializeToString(), b''])
    data = recv_lead_node.recv()
    
    return send_file(io.BytesIO(data), mimetype=res['content_type'])

if __name__ == "__main__":
    host_local_computer = "localhost"
    host_local_network = "0.0.0.0"
    app.run(host="localhost", port=9000)