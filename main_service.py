# Основная логика сервиса

from rabbit import RabbitMain
from utils import EnvRb, load_env


if __name__ == "__main__":
    rb_env = load_env()
    rb_main = RabbitMain(**rb_env.dict())
    rb_main.start_consuming()