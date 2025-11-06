from pathlib import Path
import subprocess
import os
import json

def create_schema_and_models(file_path_config: str):
    with open(file_path_config, "r", encoding="utf-8") as f:
        schema_list = json.load(f)
    args_json = json.dumps(schema_list)
    cmd = [
        "dbt",
        "run-operation", "create_multiple_schemas",
        "--args", args_json,
        "--profiles-dir", "./sqlcreator/.dbt",
        "--project-dir", "./sqlcreator"
    ]

    print("Creating Schemas...")
    result = subprocess.run(cmd, capture_output=True, text=True)
   
    dbt_models_path = os.path.join("sqlcreator", "models")

    for schema in schema_list["schema_list"]:
        folder_path = Path(dbt_models_path) / schema
        folder_path.mkdir(parents=True, exist_ok=True)

    return schema_list["schema_list"]