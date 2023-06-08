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

def load_env():
    '''Загружает переменные окружения'''

    load_dotenv()

    # pg_dsl = DslPg(
    #     dbname=os.getenv('DBNAME_PG'),
    #     user=os.getenv('USER_PG'),
    #     password=os.getenv('PASSWORD_PG'),
    #     host=os.getenv('HOST_PG'),
    #     port=os.getenv('PORT_PG')
    # )

    rb_dsl = EnvRb(
        host=os.getenv('ASD_RMQ_HOST'),
        port=os.getenv('ASD_RMQ_PORT'),
        login=os.getenv('SERVICE_RMQ_ENOTIFY_USERNAME'),
        password=os.getenv('SERVICE_RMQ_ENOTIFY_PASSWORD'),
        virtualhost=os.getenv('ASD_RMQ_VHOST'),
        heartbeat=int(os.getenv('ASD_RMQ_HEARTBEAT')),
        queue=os.getenv('SERVICE_RMQ_QUEUE')
    )

    return rb_dsl