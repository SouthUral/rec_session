from rabbit import RabbitMain
from postgres import PostgresMain
from models import message, session
from log_pack import log_error, log_success


class sessions:
    const_mess = "DB_MSG_TYPE_SHIFT_ENTER"

    def __init__(self, db_main: PostgresMain):
        self.db_main = db_main
        self.sessions_data: dict[message.object_id : message] = self.init_active_session()

    @classmethod
    def converter(cls, data: list[list]) -> dict[int : message]:
        """
        Преобразует данные полученные из Postgres в модели
        """
        return self

    def add_message(self, message: message):
        if message.object_id not in self.sessions_data:
            self.sessions_data[message.object_id] = message
            self.add_to_db(message.object_id)
        else:
            if message.event_const == self.const_mess:
                self.sessions_data[message.object_id] = message
                self.add_to_db(message.object_id)
            else:
                session_model: message = self.sessions_data.get(message.object_id)
                session_model.av_speed = message.av_speed
                session_model.offset = message.offset
                session_model.mess_date = message.mess_date
                self.update_to_db(session_model.object_id)

    def init_active_session(self):
        sessions_data = dict()
        query = '''select 
	        distinct on (object_id)
	        object_id,
	        driver_id,
	        av_speed,
	        offset_mess,
	        mess_date,
	        id_session 
        from disp.driver_sessions
        order by object_id, mess_date desc'''
        for item in self.db_main.request_many(query):
            session = message.parse_db_obj(item)
            sessions_data[session.object_id] = session
        return sessions_data

    def add_to_db(self, object_id: int):
        query = '''
            INSERT INTO disp.driver_sessions 
            (object_id, driver_id, av_speed, offset_mess, mess_date, update_date)
            VALUES (%s, %s, %s, %s, %s, current_timestamp)
            RETURNING id_session
            '''
        model = self.sessions_data.get(object_id)
        query_params: tuple = model.format_to_db()
        answer = self.db_main.request_to_table(query, query_params, p_return=True)[0]
        model.session_id = answer

    def update_to_db(self, object_id: int):
        query = '''
            UPDATE disp.driver_sessions 
            SET av_speed = %s, offset_mess = %s, mess_date = %s, update_date = current_timestamp
            WHERE id_session = %s
            '''
        model = self.sessions_data.get(object_id)
        query_params: tuple = model.format_to_update()
        self.db_main.request_to_table(query, query_params)
        

class ManagerDBModels: 
    """
    Класс предназначен для управления данными полученными из БД
    """

    def __init__(self, db_main: PostgresMain):
        self.db_main = db_main

        self.technic: dict[int : const] = self._get_technic()
        self.driver_on_tech: dict[int : int] = self._get_driver_on_tech()
        self.active_sessions: sessions = self._get_active_sessions()

    def _get_active_sessions(self):
        """
        Получает из БД активные сессии
        """
        return sessions(self.db_main)

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
        self.active_sessions.add_message(message)


class verifier:

    const_tech = "TSARDOM_HAUL_TRUCKS"

    def __init__(self, models_main: ManagerDBModels):
        self.models_main = models_main

    def checking_message(self, message: message):
        """
        Метод проверяет сообщение
        """
        check_methods = [
            self._check_technic,
            self._check_speed_key
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
        mess_model: message = message.parse_message(body, offset)
        if self.verifier.checking_message(mess_model):
            self.db_models_main.add_message(mess_model)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def __call__(self):
        self.rb_main.start_consuming(self.callback)

