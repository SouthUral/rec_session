import os
from dotenv import load_dotenv
from pydantic import BaseModel

class EnvRb(BaseModel):
    host: str
    port: str
    login: str
    password: str
    virtualhost: str
    heartbeat: int
    queue: str

class EnvDb(BaseModel):
    dbname: str
    user: str
    password: str
    host: str
    port: int

class Env(BaseModel):
    rb_dsn: EnvRb
    pg_dsn: EnvDb
    pg_dsn_bgp: EnvDb

def load_env():
    '''Загружает переменные окружения'''

    load_dotenv()

    pg_dsn_bgp = EnvDb(
        dbname=os.getenv('ASD_POSTGRES_DBNAME'),
        user=os.getenv('SEVICE_REC_SESSION_USERNAME'),
        password=os.getenv('SEVICE_REC_SESSION_PASSWORD'),
        host=os.getenv('ASD_POSTGRES_HOST'),
        port=os.getenv('ASD_POSTGRES_PORT')
    )

    rb_dsn = EnvRb(
        host=os.getenv('ASD_RMQ_HOST'),
        port=os.getenv('ASD_RMQ_PORT'),
        login=os.getenv('SERVICE_RMQ_ENOTIFY_USERNAME'),
        password=os.getenv('SERVICE_RMQ_ENOTIFY_PASSWORD'),
        virtualhost=os.getenv('ASD_RMQ_VHOST'),
        heartbeat=int(os.getenv('ASD_RMQ_HEARTBEAT')),
        queue=os.getenv('SERVICE_RMQ_QUEUE')
    )

    pg_dsn = EnvDb(
        dbname=os.getenv('DBNAME_PG'),
        user=os.getenv('USER_PG'),
        password=os.getenv('PASSWORD_PG'),
        host=os.getenv('HOST_PG'),
        port=os.getenv('PORT_PG')
    )

    res_env = Env(
        rb_dsn=rb_dsn,
        pg_dsn=pg_dsn,
        pg_dsn_bgp=pg_dsn_bgp
    )

    return res_env