import os
import sys
import threading
import uuid
import subprocess
from urllib.parse import urlparse
from flask import Flask
from amqpstorm import Connection, Message

app = Flask(__name__)

# ------------------------------------------------------------
# 1. Función para parsear la URL de CloudAMQP
# ------------------------------------------------------------
def parse_amqp_url(url):
    parsed = urlparse(url)
    host = parsed.hostname
    port = parsed.port or (5671 if parsed.scheme == 'amqps' else 5672)
    username = parsed.username
    password = parsed.password
    vhost = parsed.path[1:] if parsed.path and parsed.path != '/' else '/'
    return host, port, username, password, vhost

# ------------------------------------------------------------
# 2. Cliente RPC (igual que antes, pero adaptado a URL)
# ------------------------------------------------------------
class RpcClient:
    def __init__(self):
        amqp_url = os.environ.get('CLOUDAMQP_URL')
        if amqp_url:
            host, port, username, password, vhost = parse_amqp_url(amqp_url)
            self.connection = Connection(host, username, password,
                                         port=port, virtual_host=vhost)
        else:
            # Modo desarrollo local
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

# ------------------------------------------------------------
# 3. Worker RPC (se ejecuta en un proceso hijo)
# ------------------------------------------------------------
def start_rpc_server():
    # Lanza rpc_server.py en segundo plano
    subprocess.Popen([sys.executable, "rpc_server.py"])

# Iniciamos el worker tan pronto como se carga el módulo
threading.Thread(target=start_rpc_server, daemon=True).start()

# ------------------------------------------------------------
# 4. Instancia global del cliente RPC
# ------------------------------------------------------------
rpc_client = RpcClient()

# ------------------------------------------------------------
# 5. Ruta de la API
# ------------------------------------------------------------
@app.route('/rpc_call/<payload>')
def rpc_call(payload):
    corr_id = rpc_client.send_request(payload)
    if rpc_client.queue[corr_id]['event'].wait(timeout=10.0):
        return rpc_client.queue[corr_id]['data']
    return "Error: timeout", 504

# ------------------------------------------------------------
# 6. Punto de entrada para Gunicorn (producción) y desarrollo
# ------------------------------------------------------------
if __name__ == '__main__':
    # Ejecución local con el servidor de desarrollo de Flask
    app.run(debug=True, port=5000)
else:
    # Cuando Gunicorn importa la aplicación, se ejecuta este bloque.
    # Aseguramos que el worker esté corriendo (ya se lanzó arriba)
    # y mostramos un mensaje en los logs de Render.
    port = int(os.environ.get('PORT', 10000))
    print(f"API Flask lista. Escuchando en el puerto {port} (0.0.0.0)", file=sys.stderr)