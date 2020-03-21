import requests
import json
import pandas as pd


class Toloka:
    def __init__(self, token):
        self.toloka_url = "https://toloka.yandex.ru"
        self.toloka_token = token

    def get_pool_answers(self, pool_id):
        task_answers = requests.get(
            f"{self.toloka_url}/api/v1/assignments",
            params={"pool_id": pool_id, "limit": "10000",},
            headers={
                "Authorization": f"OAuth {self.toloka_token}",
                "Content-Type": "application/JSON",
            },
        )

        return build_answers_dataframe(task_answers.json())


def build_answers_dataframe(data):
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
