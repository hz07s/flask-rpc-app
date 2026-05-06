import os
import sys
import threading
import uuid
import traceback
from urllib.parse import urlparse
from flask import Flask
from amqpstorm import Connection, Message

app = Flask(__name__)

# ------------------------------------------------------------
# 1. Función para parsear la URL de CloudAMQP (Adaptada para WebSockets)
# ------------------------------------------------------------
def parse_amqp_url(url):
    parsed = urlparse(url)
    # Forzar el uso del puerto 443 y el esquema 'wss' (WebSockets Seguro)
    scheme = 'wss'
    port = 443
    host = parsed.hostname
    username = parsed.username
    password = parsed.password
    vhost = parsed.path[1:] if parsed.path and parsed.path != '/' else '/'
    return host, port, username, password, vhost, scheme

# ------------------------------------------------------------
# 2. Cliente RPC Mejorado con Logs Exhaustivos
# ------------------------------------------------------------
class RpcClient:
    def __init__(self):
        print("[DEBUG] Inicializando RpcClient...", file=sys.stderr)
        amqp_url = os.environ.get('CLOUDAMQP_URL')
        if not amqp_url:
            print("[ERROR] La variable de entorno CLOUDAMQP_URL no está configurada.", file=sys.stderr)
            sys.exit(1)
            
        try:
            print(f"[DEBUG] URL obtenida: {amqp_url}", file=sys.stderr)
            host, port, username, password, vhost, scheme = parse_amqp_url(amqp_url)
            print(f"[DEBUG] Conectando a {host}:{port} como usuario {username}...", file=sys.stderr)
            
            # Configuración de conexión para usar WebSockets (puerto 443)
            self.connection = Connection(
                host, username, password,
                port=port,
                virtual_host=vhost,
                ssl=True,
                ssl_options={'cert_reqs': 'CERT_REQUIRED'}
            )
            print("[DEBUG] Conexión AMQP establecida correctamente.", file=sys.stderr)
            
            self.channel = self.connection.channel()
            result = self.channel.queue.declare(exclusive=True, auto_delete=True)
            self.callback_queue = result['queue']
            self.channel.basic.consume(self._on_response, queue=self.callback_queue, no_ack=True)
            self.queue = {}
            self.thread = threading.Thread(target=self._start_consuming)
            self.thread.daemon = True
            self.thread.start()
            print("[DEBUG] Cliente RPC inicializado y a la espera de respuestas.", file=sys.stderr)
        except Exception as e:
            print(f"[ERROR FATAL] No se pudo inicializar RpcClient: {e}", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
            sys.exit(1)

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
# 3. Instancia global del cliente RPC
# ------------------------------------------------------------
print("[DEBUG] Creando instancia global de RpcClient...", file=sys.stderr)
rpc_client = RpcClient()

# ------------------------------------------------------------
# 4. Ruta de la API Flask
# ------------------------------------------------------------
@app.route('/rpc_call/<payload>')
def rpc_call(payload):
    try:
        corr_id = rpc_client.send_request(payload)
        if rpc_client.queue[corr_id]['event'].wait(timeout=15.0):
            return rpc_client.queue[corr_id]['data']
        else:
            return "Error: Timeout - El servidor RPC no respondió a tiempo.", 504
    except Exception as e:
        print(f"[ERROR] En ruta rpc_call: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        return f"Error interno del servidor: {e}", 500

# ------------------------------------------------------------
# 5. Punto de entrada para ejecutar la aplicación
# ------------------------------------------------------------
if __name__ == '__main__':
    app.run(debug=True, port=5000)
else:
    # Este bloque se ejecutará cuando Gunicorn importe la app en Render
    port = int(os.environ.get('PORT', 10000))
    print(f"[DEBUG] Aplicación iniciada por Gunicorn. Escuchando en 0.0.0.0:{port}", file=sys.stderr)