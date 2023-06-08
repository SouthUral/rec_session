import pika
from log_pack import log_error, log_success

class RabbitMain:
    """
    Основной класс для подключения к Rabbit
    """
    def __init__(self, host, port, login, password, virtualhost, heartbeat, queue, offset="last"):
        self.host = host
        self.port = port
        self.login = login
        self.password = password
        self.virtualhost = virtualhost
        self.heartbeat = heartbeat
        self.offset=offset
        self.queue = queue
        self.url_parametrs = self.get_url()

    def get_url(self):
        url_parametrs = f"amqp://{self.login}:{self.password}@{self.host}:{self.port}/{self.virtualhost}"
        return url_parametrs

    def start_consuming(self):
        self.conn, self.channel = self._connection()
        self.channel.basic_qos(prefetch_count=10)
        try:
            self.channel.basic_consume(
                queue=self.queue, 
                auto_ack=False,
                on_message_callback=self.callback,
                arguments={"x-stream-offset": "last"},
                consumer_tag="rec_session"
                )
        except Exception as err:
            log_error(err)
            log_error("consume failed")
        try:
            self.channel.start_consuming()
        except Exception as err:
            log_error(err)
            log_error("start consuming failed")

    def _connection(self):
        try:
            rmq_parameters = pika.URLParameters(self.url_parametrs)
            conn = pika.BlockingConnection(rmq_parameters)
            channel = conn.channel()
            return conn, channel
            log_success("RabbitMQ init complete")
        except Exception as err:
            log_error(err)

    @staticmethod
    def callback(ch, method, properties, body):
        print(" [x] Received %r" % body)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        

