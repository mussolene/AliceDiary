import datetime
import os

import ydb
import ydb.iam

from skill.schemas import PlannedLesson

fillingQuery = (
    "DECLARE $lesson_homework AS List<Struct<",
    "    date: Date,",
    "    lesson: Utf8,",
    "    homework: Utf8>>;",
    "",
    "REPLACE INTO PlannedLesson",
    "SELECT",
    "    currentUTCDateTime() as created_at,",
    "    date,",
    "    lesson as subject_name,",
    "    homework",
    "FROM AS_TABLE($lesson_homework);",
)

driver = ydb.Driver(
    endpoint=os.environ.get("YDB_ENDPOINT"),
    database=os.environ.get("YDB_DATABASE"),
    credentials=ydb.iam.ServiceAccountCredentials.from_file(
        os.environ.get("YDB_SA_FILE"),
    ),
)

driver.wait(fail_fast=True, timeout=5)

pool = ydb.SessionPool(driver)


def retry_query(session, query):
    return session.transaction().execute(
        query,
        commit_tx=True,
        settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2),
    )


def retry_prepared_query(session, query, lesson_homework):
    prepared_query = session.prepare(query)
    return session.transaction(ydb.SerializableReadWrite()).execute(
        prepared_query,
        {
            "$lesson_homework": lesson_homework,
        },
        commit_tx=True,
    )


def get_all(yid: str):
    query = f""" $format = DateTime::Format("%Y-%m-%d");
                SELECT * FROM PlannedLesson WHERE YID='{yid}';"""

    result = pool.retry_operation_sync(retry_query, query=query)

    return result[0].rows


def get_lessons_from_time(yid: str, date: datetime):
    query = f""" $format = DateTime::Format("%Y-%m-%d");
                SELECT lesson_num
                    ,subject_name
                    ,date
                FROM PlannedLesson 
                    WHERE YID='{yid}'
                        AND date = {date};"""

    result = pool.retry_operation_sync(retry_query, query=query)

    return result[0].rows


def get_homework_from_time(yid: str, date: datetime):
    query = f""" $format = DateTime::Format("%Y-%m-%d");
                SELECT date
                    ,subject_name
                    ,homework
                FROM PlannedLesson 
                    WHERE YID='{yid}'
                        AND date = {date};"""

    result = pool.retry_operation_sync(retry_query, query=query)

    return result[0].rows


def table_filling(lesson_homework: list):
    pool.retry_operation_sync(
        retry_prepared_query, query=fillingQuery, lesson_homework=lesson_homework
    )
