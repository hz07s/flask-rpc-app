# Aplicación RPC con Flask, RabbitMQ y CloudAMQP

## Despliegue en Render

1. Crear instancia gratuita en CloudAMQP y obtener la URL `amqp://...`
2. En Render, crear un **Web Service** conectado a este repo.
3. Añadir variable de entorno: `CLOUDAMQP_URL` = (la URL de CloudAMQP)
4. Crear también un **Background Worker** (otro servicio) con el comando:
   `python rpc_server.py`
   y la misma variable de entorno.
5. Ambos servicios se conectarán a la misma cola y funcionarán.

## Ejecución local

1. Instalar dependencias: `pip install -r requirements.txt`
2. Levantar RabbitMQ local (o usar CloudAMQP) y ejecutar:
   - `python rpc_server.py`
   - `python amqpstorm_threaded_rpc_client.py`
3. Probar con `curl http://localhost:5000/rpc_call/Hola`