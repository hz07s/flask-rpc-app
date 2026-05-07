import os
import threading
import uuid
from urllib.parse import urlparse
from flask import Flask
from amqpstorm import Connection, Message

app = Flask(__name__)

def parse_amqp_url(url):
    parsed = urlparse(url)
    host = parsed.hostname
    port = parsed.port or 5672
    username = parsed.username
    password = parsed.password
    vhost = parsed.path[1:] if parsed.path and parsed.path != '/' else '/'
    return host, port, username, password, vhost

class RpcClient:
    def __init__(self):
        amqp_url = os.environ.get('CLOUDAMQP_URL')
        if amqp_url:
            host, port, username, password, vhost = parse_amqp_url(amqp_url)
            self.connection = Connection(host, username, password, port=port, virtual_host=vhost)
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

rpc_client = RpcClient()

@app.route('/rpc_call/<payload>')
def rpc_call(payload):
    corr_id = rpc_client.send_request(payload)
    if rpc_client.queue[corr_id]['event'].wait(timeout=15.0):
        return rpc_client.queue[corr_id]['data']
    return "Error: timeout", 504

if __name__ == '__main__':
    app.run(debug=True, port=5000)