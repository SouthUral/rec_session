from rabbit import RabbitMain
from postgres import PostgresMain
from models import message, session
from log_pack import log_error, log_success


class sessions:

    def __init__(self, db_main: PostgresMain, data):
        self.db_main = db_main
        self.data = data

    @classmethod
    def converter(cls, data: list[list]) -> dict[object_id: message]:
        """
        Преобразует данные полученные из Postgres в модели
        """
        pass


class ManagerDBModels:
    """
    Класс предназначен для управления данными полученными из БД
    """

    const_mess = "DB_MSG_TYPE_SHIFT_ENTER"

    def __init__(self, db_main: PostgresMain):
        self.db_main = db_main

        self.technic: dict[object_id : const] = self._get_technic()
        self.driver_on_tech: dict[object_id : driver_id] = self._get_driver_on_tech()
        self.active_sessions: dict[object_id : session] = self._get_active_sessions()

    def _get_active_sessions(self):
        """
        Получает из БД активные сессии
        """
        query = "select object_id, const from sh_data.v_technics where not not_used"
        return dict()
        # сначала нужно закинуть туда пару сообщений

    def _get_technic(self) -> dict:
        """
        Получает из БД список техники
        """
        query = "select object_id, const from sh_data.v_technics where not not_used"
        units = {item[0]: item[1] for item in self.db_main.request_many(query)}
        return units

    def _get_driver_on_tech(self) -> dict:
        """
        Получает список техники и id пользователей на технике
        """
        query = "select object_id, driver_id from sh_disp.v_driver_on_tech"
        units = {item[0]: item[1] for item in self.db_main.request_many(query)}
        return units

    def update_technic(self):
        self.technic: dict[object_id : const] = self._get_technic()

    def add_message(self, message: message):
        message.driver_id = self.driver_on_tech.get(message.object_id)
        # model_session: session = session.parse_message(message, driver_id)
        if message.event_const == self.const_mess:

    # def request_switch(self, message):
    #     if message.event_const == self.const_mess:
    #         pass


class verifier:

    const_tech = "TSARDOM_HAUL_TRUCKS"
    const_mess = "DB_MSG_TYPE_SHIFT_ENTER"

    def __init__(self, models_main: ManagerDBModels):
        self.models_main = models_main

    def checking_message(self, message: message):
        """
        Метод проверяет сообщение
        """
        check_methods = [
            self.check_technic,
            self.check_speed_key
        ]

        return all(map(lambda func: func(message), check_methods))

    def _check_technic(self, message: message, re_checking: bool=False) -> bool:
        """
        Производит проверку на наличие id техники в списке техники
        Далее проверяет соответствует ли константа
        """

        if message.object_id not in self.models_main.technic:
            if re_checking:
                log_error("the new technic is not registered in the table")
                return False
            self.models_main.update_technic()
            return self.check_technic(message, re_checking=True)

        if self.models_main.technic.get(message.object_id) != self.const_tech:
            return False

        return True

    def _check_speed_key(self, message: message):
        return True if message.av_speed else False


class RecSessions:

    def __init__(self, rb_main: RabbitMain, db_main: PostgresMain):
        self.rb_main = rb_main
        self.db_models_main = ManagerDBModels(db_main)
        self.verifier = verifier(self.db_models_main)
        
    def callback(self, ch, method, properties, body):
        """
        Основная логика будет вызываться отсюда
        """
        offset = properties.headers["x-stream-offset"]
        mess_model: message = message.parse_data(body, offset)
        if self.verifier.checking_message(mess_model):
            self.db_models_main.add_message(mess_model)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def __call__(self):
        self.rb_main.start_consuming(self.callback)

