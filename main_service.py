# Основная логика сервиса

import json

from rabbit import RabbitMain
from postgres import PostgresMain
from utils import EnvRb, load_env, Env
from models import parse_mess, message
from algo import RecSessions


def json_calllback(ch, method, properties, body):
    mess_model = parse_mess(body)
    print(mess_model)
    ch.basic_ack(delivery_tag=method.delivery_tag)

if __name__ == "__main__":
    env_dsn: Env = load_env()
    db_main = PostgresMain(**env_dsn.pg_dsn_bgp.dict())
    rb_main = RabbitMain(**env_dsn.rb_dsn.dict())
    rb_main.offset = db_main.get_offset()
    master_sessions = RecSessions(rb_main=rb_main, db_main=db_main)
    master_sessions()
