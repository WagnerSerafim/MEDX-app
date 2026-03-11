import json
import os
from datetime import date, datetime


def _json_serializer(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value)


def write_jsonl_log(records, log_folder, filename):
    os.makedirs(log_folder, exist_ok=True)
    file_path = os.path.join(log_folder, filename)

    with open(file_path, "w", encoding="utf-8") as file:
        for record in records:
            file.write(json.dumps(record, ensure_ascii=False, default=_json_serializer))
            file.write("\n")

    return file_path
