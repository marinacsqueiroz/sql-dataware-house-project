from pathlib import Path
from logging import Logger
import subprocess
import os
import json

def create_schema_and_models(file_path_config: str, logger: Logger) -> bool:
    
    logger.info(f"Loading schema list from: {file_path_config}")
    try:
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
        
        logger.debug(f"Executing DBT command: {' '.join(cmd)}")
        
        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            logger.error("Failed to execute DBT operation 'create_multiple_schemas'.")
            logger.error(f"STDOUT: {result.stdout.strip()}")
            logger.error(f"STDERR: {result.stderr.strip()}")
            return False
        
        logger.info("DBT operation 'create_multiple_schemas' completed successfully.")
        logger.debug(f"DBT Output: {result.stdout.strip()}")

        dbt_models_path = os.path.join("sqlcreator", "models")
        
        for schema in schema_list["schema_list"]:
            folder_path = Path(dbt_models_path) / schema
            folder_path.mkdir(parents=True, exist_ok=True)
            logger.debug(f"Model folder created: {folder_path}")

        return True

    except FileNotFoundError:
        logger.error(f"Configuration file not found: {file_path_config}")
        return False
    except json.JSONDecodeError:
        logger.error(f"Error decoding JSON in file: {file_path_config}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error in create_schema_and_models function: {e}", exc_info=True)
        return False