import zmq
import time
import json
import logging

class Publisher:
    def __init__(self, address="tcp://*:5555"):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUB)
        self.socket.bind(address)

    def publish(self, data):
        while True:
            message = json.dumps(data)
            self.socket.send_string(message)


class Subscriber:
    def __init__(self):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.SUB)
        self.socket.connect("tcp://localhost:5555")
        self.socket.setsockopt_string(zmq.SUBSCRIBE, '')  # Subscribe to all messages

    def listen(self):
        while True:
            message = self.socket.recv_string()
            data = json.loads(message)
            print(f"Received: {data}")