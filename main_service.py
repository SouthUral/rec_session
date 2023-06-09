# Основная логика сервиса

import json

from rabbit import RabbitMain
from postgres import PostgresMain
from utils import EnvRb, load_env, Env
from models import parse_mess


def json_calllback(ch, method, properties, body):
    mess_model = parse_mess(body)
    print(mess_model)
    ch.basic_ack(delivery_tag=method.delivery_tag)

if __name__ == "__main__":
    env_dsn: Env = load_env()
    rb_main = RabbitMain(**env_dsn.rb_dsn.dict())
    db_main = PostgresMain(**env_dsn.pg_dsn.dict())
    rb_main.init_offset(db_main.get_offset())
    rb_main.start_consuming(db_main.pg_callback)
    # rb_main.start_consuming(json_calllback)