import requests
import json
import pandas as pd
from typing import Union


class Toloka:
    def __init__(self, token):
        '''
        Это документация в конструкторе.

        `token` - OAuth токен Толоки, который можно получить в личном кабинете.
        '''

        self._toloka_url = "https://toloka.yandex.ru"
        self._toloka_token = token

    def get_pool_answers(self, pool_id: Union[str, int]) -> pd.DataFrame:
        '''
        Получение всех ответов в заданном пуле.

        `pool_id` - идентификатор пула

        Возвращает `pandas.DataFrame` со столбцами:

        * `toloka_*` - аттрибуты в привязке к внутренней логике Толоки
        * `input_*` - поля входных данных
        * `output_*` - поля выходных данных, результаты разметки
        '''

        task_answers = requests.get(
            f"{self._toloka_url}/api/v1/assignments",
            params={"pool_id": pool_id, "limit": "10000",},
            headers={
                "Authorization": f"OAuth {self._toloka_token}",
                "Content-Type": "application/JSON",
            },
        )

        return _build_answers_dataframe(task_answers.json())


def _build_answers_dataframe(data):
    records = []
    # выдачи наборов заданий
    for item in data["items"]:
        toloka_details = {
            "toloka_id": item["id"],
            "toloka_suite_id": item["task_suite_id"],
            "toloka_pool_id": item["pool_id"],
            "toloka_user_id": item["user_id"],
            "toloka_status": item["status"],
            "toloka_reward": item["reward"],
        }
        if ("tasks" not in item) or ("solutions" not in item):
            continue
        for task, solution in zip(item["tasks"], item["solutions"]):
            task_details = {
                "task_id": task["id"],
            }
            for key, value in task["input_values"].items():
                task_details[f"input_{key}"] = value
            for key, value in solution["output_values"].items():
                task_details[f"output_{key}"] = value
            record = dict(toloka_details, **task_details)
            records.append(record)
    return pd.DataFrame.from_records(records)
