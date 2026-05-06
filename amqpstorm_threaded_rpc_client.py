import os
import threading
import uuid
from flask import Flask
from amqpstorm import Connection, Message

app = Flask(__name__)

class RpcClient:
    def __init__(self):
        # Obtener URL de RabbitMQ desde variable de entorno
        self.cloudamqp_url = os.environ.get('CLOUDAMQP_URL')
        if self.cloudamqp_url:
            self.connection = Connection(self.cloudamqp_url)
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
        message = Message.create(self.channel, payload)
        message.correlation_id = corr_id
        message.reply_to = self.callback_queue
        message.publish(routing_key='rpc_queue')
        return corr_id

RPC_CLIENT = RpcClient()

@app.route('/rpc_call/<payload>')
def rpc_call(payload):
    corr_id = RPC_CLIENT.send_request(payload)
    if RPC_CLIENT.queue[corr_id]['event'].wait(timeout=10.0):
        return RPC_CLIENT.queue[corr_id]['data']
    else:
        return "Error: timeout", 504

# Para ejecución local con python directo (no necesario en producción con gunicorn)
if __name__ == '__main__':
    app.run(debug=True, port=5000)