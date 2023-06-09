import json
from pydantic import BaseModel
from datetime import date, datetime

class message(BaseModel):
    object_id: int
    av_speed: float | None
    mess_date: datetime
    offset: int | None

    def format_to_db(self):
        object_id = str(self.object_id)
        av_speed = str(self.av_speed)
        offset = str(self.offset)
        mess_date = self.mess_date.strftime("%Y-%m-%d %H:%M:%S.%f")
        # return f'{object_id}, {av_speed}, {mess_date}'
        return object_id, av_speed, mess_date, offset

def parse_mess(data: str):
    data = json.loads(data)
    model = message(
        object_id=data.get("object_id"),
        av_speed=data.get("data").get("s_av_speed", None),
        mess_date=datetime.strptime(data.get("mes_time"), "%Y-%m-%dT%H:%M:%S.%f")
    )
    return model