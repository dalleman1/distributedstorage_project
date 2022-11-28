import time
import zmq 
import messages_pb2


context = zmq.Context()

send_lead_node = context.socket(zmq.PUSH)
send_lead_node.bind("tcp://*:5556")

recv_lead_node = context.socket(zmq.PULL)
recv_lead_node.bind("tcp://*:5555")

time.sleep(.1)

with open('testfolder/1mb.txt', 'rb') as f:
    data = f.read()

task = messages_pb2.data_request()
task.filename = 'cool-file.txt'
task.type = 'retrive'

# send_lead_node.send_multipart([task.SerializeToString(), data])
send_lead_node.send_multipart([task.SerializeToString(), bytes('This is dum', 'utf-8')])

msg = recv_lead_node.recv()

print(msg)