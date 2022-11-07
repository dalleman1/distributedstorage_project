from flask import Flask, make_response, g, request
import sqlite3
import zmq
import time


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
send_task_socket.bind("tcp://*5557")

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
    pass


@app.route("/files/<int:file_id>", methods=["DELETE"])
def delete_file(file_id):
    # Delete from k storage nodes
    # Delete from db
    pass


@app.route("/files", methods=["POST"])
def add_files():
    # Get payload from request
    # Add to db
    # Make copies to k storage nodes

    files = request.files

    file = files.get("file")
    filename = files.filename
    content_type = file.mimetype

    data = bytearray(file.read())

    db = get_db()
    cursor = db.execute(
        """INSERT INTO file(filename, size, content_type) 
           VALUES (?, ?, ?)""",
        (filename, len(data), content_type),
    )

    db.commit()

    return make_response({"id": cursor.lastrowid}, 201)


if __name__ == "__main__":
    host_local_computer = "localhost"
    host_local_network = "0.0.0.0"
    app.run(host=host_local_computer, port=9000)
