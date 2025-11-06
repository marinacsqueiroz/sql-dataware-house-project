



import json
import os
from pathlib import Path
import subprocess


file_path_data_config = os.path.join("scripts", "config", "bronze", "column_types.json")
file_path_datasets = os.path.join("datasets")


with open(file_path_data_config, "r", encoding="utf-8") as f:
        column_type = json.load(f)

for key, item in column_type.items():
        
        args = {
            "schema": "bronze",
            "table_name": key,
            "columns": item
        }
      
        cmd = [
            "dbt",
            "run-operation", "create_table",
            "--args", json.dumps(args),
            "--profiles-dir", "./sqlcreator/.dbt",
            "--project-dir", "./sqlcreator"
        ]
   
        result = subprocess.run(cmd, capture_output=True, text=True)

        folder, csv_file = key.split("_", 1)
        csv_path = Path(file_path_datasets) / folder / f"{csv_file}.csv"
        
        if not csv_path.exists():
            print(f"⚠️  CSV não encontrado: {csv_path}")
            continue

        insert_args = {
            "file_path": str(csv_path.resolve()),
            "table_path": f"bronze.{key}"
        }

        insert_cmd = [
            "dbt", "run-operation", "inser_data",
            "--args", json.dumps(insert_args),
            "--profiles-dir", "./sqlcreator/.dbt",
            "--project-dir", "./sqlcreator"
        ]  
       
        insert_result = subprocess.run(insert_cmd, capture_output=True, text=True)
       
     

        
