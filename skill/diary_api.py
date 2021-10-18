import datetime
import os
from datetime import date, time
from typing import List

import pandas as pd
import requests

from skill.schemas import Homework, PlannedLesson

URL_DATA = "http://a.n.n3demo.ru/webservice/yandex/execute"


df = pd.read_csv(
    os.path.abspath("skill/database.csv"),
    sep=";",
    encoding="utf-8",
    dtype="str",
)
df["date"] = pd.to_datetime(df["date"], format="%d.%m.%Y")
df["time_start"] = pd.to_datetime(df["time_start"], format="%H:%M")
df["time_end"] = pd.to_datetime(df["time_end"], format="%H:%M")
df["lesson"] = df["lesson"].str.lower()
df.fillna("", inplace=True)


class NotFoundError(Exception):
    pass


def get_real_schedule(
    school_id: str, class_id: str, day=None, lessons=[]
) -> List[PlannedLesson]:

    if day is None:
        day = date.today()

    response = requests.get(
        URL_DATA,
        params={
            "action": "schedule",
            "class": class_id,
            "date": datetime.datetime.strftime(day, "%Y-%m-%d"),
        },
    )

    if response.status_code == 500:
        return []

    result = [
        PlannedLesson(lesson["subject_name"], None, None, 1)
        for lesson in response.json()
    ]
    return result


def get_real_homework(
    school_id: str, class_id: str, day=None, lessons=[]
) -> List[Homework]:
    if day is None:
        day = date.today() + datetime.timedelta(days=1)

    response = requests.get(
        URL_DATA,
        params={
            "action": "hometask",
            "class": class_id,
            "date": datetime.datetime.strftime(day, "%Y-%m-%d"),
        },
    )

    if response.status_code == 500:
        return []

    result = [
        Homework(lesson["subject_name"], lesson["hometask"])
        for lesson in response.json()
        if (not lessons or lesson["subject_name"].lower() in lessons)
        and lesson["hometask"]
    ]
    return result


def get_schedule(
    school_id: str, class_id: str, day=None, lessons=[]
) -> List[PlannedLesson]:

    all_schedule = df.loc[
        (df["school_id"] == str(school_id)) & (df["class_id"] == class_id.lower())
    ]
    if day is None:
        day = date.today()

    schedule = __schedule(all_schedule, day, lessons)
    result = []
    prev = None
    prev_lesson = None
    for index, lesson in schedule.iterrows():
        if lesson["lesson"] == prev:
            prev_lesson.inc()
        else:
            prev = lesson["lesson"]
            prev_lesson = PlannedLesson(lesson["lesson"], None, None, 1)
            result.append(prev_lesson)

    return result


def __schedule(schedule: pd.DataFrame, day: date, lessons: list) -> pd.DataFrame:
    lessons_filter = [x.lower() for x in lessons]
    return schedule.loc[
        (schedule["date"] == pd.Timestamp(datetime.datetime.combine(day, time())))
        & (not lessons or schedule["lesson"].isin(lessons_filter))
    ]


def get_homework(school_id: str, class_id: str, day=None, lessons=[]) -> List[Homework]:
    all_schedule = df.loc[
        (df["school_id"] == str(school_id)) & (df["class_id"] == class_id.lower())
    ]

    if day is not None and lessons:
        hw = __homework_day_lessons(all_schedule, day, lessons)
    elif day is not None and not lessons:
        hw = __homework_day_no_lessons(all_schedule, day)
    elif day is None and lessons:
        hw = __homework_no_day_lessons(all_schedule, lessons)
    elif day is None and not lessons:
        hw = __homework_no_day_no_lessons(all_schedule)
    else:
        raise ValueError("Этого быть не может")

    homework = [
        Homework(row["lesson"], row["homework"]) for index, row in hw.iterrows()
    ]
    return homework


def __homework_day_lessons(schedule: pd.DataFrame, day: date, lessons: list):
    return __slice_schedule(schedule, day, lessons)


def __homework_day_no_lessons(schedule: pd.DataFrame, day: date):
    lessons = [row["lesson"] for index, row in __schedule(schedule, day, []).iterrows()]
    return __slice_schedule(schedule, day, lessons)


def __homework_no_day_lessons(schedule: pd.DataFrame, lessons: list):
    tomorrow = date.today() + datetime.timedelta(days=1)
    return __slice_schedule(schedule, tomorrow, lessons)


def __homework_no_day_no_lessons(schedule: pd.DataFrame):
    today = date.today()
    tomorrow = today + datetime.timedelta(days=1)
    lessons = [
        row["lesson"] for index, row in __schedule(schedule, today, []).iterrows()
    ] + [row["lesson"] for index, row in __schedule(schedule, tomorrow, []).iterrows()]

    return __slice_schedule(schedule, tomorrow, lessons)


def __slice_schedule(schedule: pd.DataFrame, day: date, lessons: list):
    lessons_filter = [x.lower() for x in lessons]
    df_prev = schedule.loc[
        (schedule["date"] <= pd.Timestamp(day))
        & (schedule["lesson"].isin(lessons_filter))
    ]
    homework_group = df_prev.groupby("lesson").agg({"date": "max"})
    homework_slice = schedule.merge(homework_group, on=["date", "lesson"], how="inner")
    homework = homework_slice.loc[(homework_slice["homework"] != "")]

    return homework
