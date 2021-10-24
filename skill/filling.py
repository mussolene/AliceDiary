import json
from datetime import date, datetime, time, timedelta

import diary_api
import requests
import yandex_api

today = date.today()
day_start = today - timedelta(days=7)
day_end = today + timedelta(days=7)

array_of_hw = []
class_id = "6 г"
school_id = 509

while day_start < day_end:
    day_start = day_start + timedelta(days=1)

    data_hw = diary_api.get_real_homework(
        school_id=school_id, class_id=class_id, day=day_start
    )
    data_sc = diary_api.get_real_schedule(
        school_id=school_id, class_id=class_id, day=day_start
    )

    for i in data_sc:
        new_string = {}
        new_string["date"] = day_start
        new_string["lesson"] = i.name
        new_string["school_id"] = school_id
        new_string["class_id"] = class_id
        new_string["homework"] = ""
        for j in data_hw:
            if i.name.lower() in j.lesson.lower():
                if new_string["homework"]:
                    new_string["homework"] = new_string["homework"] + "\n" + j.task
                else:
                    new_string["homework"] = j.task
        if new_string:
            array_of_hw.append(new_string)

yandex_api.table_filling(array_of_hw)
