import psycopg2
from psycopg2.extras import DictCursor, execute_values
from log_pack import log_error, log_success
from contextlib import contextmanager

from models import parse_mess, message
from utils import EnvRb, load_env, Env

class PostgresMain:
    """
    Основной класс для подключения к Postgres
    """
    
    def __init__(self, dbname: str, user: str, password: str, host: str, port: int):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.conn = self.connect()
        self.curs = self.conn.cursor()

    def connect(self):
        try:
            connect = psycopg2.connect(**self.__dict__, cursor_factory=DictCursor)
            return connect
        except Exception as err:
            log_error("Connect to DB error")
            log_error(err, exit=True)

    def get_offset(self):
        self.curs.execute("SELECT id_offset FROM disp.av_speed_data ORDER BY id_offset DESC LIMIT 1")
        offset = self.curs.fetchone()
        return offset[0]

    def request_many(self, query_txt: str) -> list[list]:
        self.curs.execute(query_txt)
        data: list = self.curs.fetchall()
        return data


    def add_to_table(self, cursor, data: message):
        request = "INSERT INTO disp.av_speed_data (object_id, av_speed, mess_date, id_offset) VALUES (%s, %s, %s, %s)"
        if not data.av_speed:
            return
        try:
            # execute_values(cursor, request, data.format_to_db())
            format_db = data.format_to_db()
            cursor.execute(request, format_db)
            log_success("data is recording")
            self.conn.commit()
        except Exception as err:
            if self.curs:
                self.curs.close()
            if self.conn:
                self.conn.close()
            log_error(err, exit=True)

    def pg_callback(self, ch, method, properties, body):
        offset = properties.headers["x-stream-offset"]
        mess_model: message = parse_mess(body)
        mess_model.offset = offset
        self.add_to_table(self.curs, mess_model)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    @staticmethod
    @contextmanager
    def conn_context(dsn: dict, cursor_factory: DictCursor):
        '''Контекстный менеджер, сделан для обработки ошибок'''
        try:
            conn = psycopg2.connect(**dsn, cursor_factory=cursor_factory)
            log_success('Connect to the postgresql database')
        except psycopg2.OperationalError as err:
            log_error(err)
            raise SystemExit
        yield conn
        conn.close()
        log_success('Connection to postgresql database is closed')
