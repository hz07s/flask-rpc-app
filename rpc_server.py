import os
import amqpstorm
from amqpstorm import Message

def on_request(message):
    payload = message.body.decode('utf-8')
    print(f" [.] Procesando: {payload}")
    response_data = f"Respuesta desde el servidor: {payload.upper()}"
    response = Message.create(message.channel, response_data)
    response.correlation_id = message.correlation_id
    response.publish(routing_key=message.reply_to)
    message.ack()

# Obtener la URL de RabbitMQ desde la variable de entorno (CloudAMQP)
CLOUDAMQP_URL = os.environ.get('CLOUDAMQP_URL')
if CLOUDAMQP_URL:
    connection = amqpstorm.Connection(CLOUDAMQP_URL)
else:
    # Fallback para desarrollo local
    connection = amqpstorm.Connection('127.0.0.1', 'guest', 'guest')

channel = connection.channel()
# Cola durable para compatibilidad con RabbitMQ 4.x
channel.queue.declare(queue='rpc_queue', durable=True, auto_delete=False, exclusive=False)
channel.basic.consume(on_request, queue='rpc_queue')

print(" [x] Servidor RPC iniciado. Esperando peticiones...")
channel.start_consuming()