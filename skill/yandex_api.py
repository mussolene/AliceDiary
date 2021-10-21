import ydb
import ydb.iam

driver = ydb.Driver(
    endpoint="grpcs://ydb.serverless.yandexcloud.net:2135",
    database="/ru-central1/b1ggjogcottkqr496da2/etn3ds48uqshhaefqhbj",
    credentials=ydb.iam.ServiceAccountCredentials.from_file(
        "sa_authorized.json",
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


query = """ $format = DateTime::Format("%Y-%m-%d");
            SELECT * FROM PlannedLesson;
        """

result = pool.retry_operation_sync(retry_query, query=query)

print(result[0].rows)
