import json
from pydantic import BaseModel
from datetime import date, datetime

class message(BaseModel):
    object_id: int
    driver_id: int | None
    av_speed: float | None
    mess_date: datetime
    offset: int | None
    event_const: str | None

    @classmethod
    def parse_data(cls, data: str, offset_msg: int):
        obj = json.loads(data)
        parse_data = {
            "object_id": obj.get("object_id"),
            "av_speed": obj.get("data").get("s_av_speed", None),
            "mess_date": datetime.strptime(obj.get("mes_time"), "%Y-%m-%dT%H:%M:%S.%f"),
            "event_const": obj.get("event_info").get("const", None),
            "offset": offset_msg
        }
        return super().parse_obj(parse_data)

    def format_to_db(self):
        object_id = str(self.object_id)
        av_speed = str(self.av_speed)
        offset = str(self.offset)
        mess_date = self.mess_date.strftime("%Y-%m-%d %H:%M:%S.%f")
        return object_id, av_speed, mess_date, offset

def parse_mess(data: str):
    data = json.loads(data)
    model = message(
        object_id=data.get("object_id"),
        av_speed=data.get("data").get("s_av_speed", None),
        mess_date=datetime.strptime(data.get("mes_time"), "%Y-%m-%dT%H:%M:%S.%f")
    )
    return model

class session(BaseModel):
    object_id: int
    driver_id: int
    av_speed: float
    offset_mess: int
    mess_date: datetime

    @classmethod
    def parse_message(cls, obj: message, driver_id: int):
        parse_data = {
            "object_id": obj.object_id,
            "driver_id": driver_id,
            "av_speed": obj.av_speed,
            "offset_mess": obj.offset,
            "mess_date": obj.mess_date
        }
        return super().parse_obj(parse_data)
