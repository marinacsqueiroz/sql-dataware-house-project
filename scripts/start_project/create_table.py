



import json
import os
import subprocess


file_path_data_config = os.path.join("scripts", "config", "bronze", "column_types.json")

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
        
        
