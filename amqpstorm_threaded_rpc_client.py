import os
import threading
import uuid
from urllib.parse import urlparse
from flask import Flask
from amqpstorm import Connection, Message
import subprocess

app = Flask(__name__)

# Función para parsear la URL de CloudAMQP
def parse_amqp_url(url):
    parsed = urlparse(url)
    host = parsed.hostname
    port = parsed.port or 5671 if parsed.scheme == 'amqps' else 5672
    username = parsed.username
    password = parsed.password
    virtual_host = parsed.path[1:] if parsed.path else '/'
    return host, port, username, password, virtual_host

class RpcClient:
    def __init__(self):
        amqp_url = os.environ.get('CLOUDAMQP_URL')
        if amqp_url:
            host, port, username, password, virtual_host = parse_amqp_url(amqp_url)
            self.connection = Connection(host, username, password, port=port, virtual_host=virtual_host)
        else:
            self.connection = Connection('127.0.0.1', 'guest', 'guest')
        self.channel = self.connection.channel()
        result = self.channel.queue.declare(exclusive=True, auto_delete=True)
        self.callback_queue = result['queue']
        self.channel.basic.consume(self._on_response, queue=self.callback_queue, no_ack=True)
        self.queue = {}
        self.thread = threading.Thread(target=self._start_consuming)
        self.thread.daemon = True
        self.thread.start()

    def _start_consuming(self):
        self.channel.start_consuming()

    def _on_response(self, message):
        corr_id = message.correlation_id
        if corr_id in self.queue:
            self.queue[corr_id]['data'] = message.body
            self.queue[corr_id]['event'].set()

    def send_request(self, payload):
        corr_id = str(uuid.uuid4())
        self.queue[corr_id] = {'data': None, 'event': threading.Event()}
        msg = Message.create(self.channel, payload)
        msg.correlation_id = corr_id
        msg.reply_to = self.callback_queue
        msg.publish(routing_key='rpc_queue')
        return corr_id

def start_rpc_server():
    subprocess.Popen(["python", "rpc_server.py"])

# Iniciar el servidor RPC en un hilo separado
threading.Thread(target=start_rpc_server, daemon=True).start()

RPC_CLIENT = RpcClient()

@app.route('/rpc_call/<payload>')
def rpc_call(payload):
    corr_id = RPC_CLIENT.send_request(payload)
    if RPC_CLIENT.queue[corr_id]['event'].wait(timeout=10.0):
        return RPC_CLIENT.queue[corr_id]['data']
    return "Error: timeout", 504

if __name__ == '__main__':
    app.run(debug=True, port=5000)