



import json
import os
from pathlib import Path
import subprocess

import json
import os
import subprocess
from pathlib import Path
from logging import Logger

def create_table(file_path_data_config: str, file_path_datasets: str, logger: Logger, add_info: bool = True) -> bool:
    
    logger.info(f"Starting table creation and data loading using config: {file_path_data_config}")
    try:
        try:
            with open(file_path_data_config, "r", encoding="utf-8") as f:
                column_type = json.load(f)

        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.error(f"Failed to load or decode configuration file {file_path_data_config}: {e}", exc_info=True)
            return False
            
        
        for key, item in column_type.items():
            table_name = key
                        
            args_create = {
                "schema": "bronze",
                "table_name": table_name,
                "columns": item
            }
            
            cmd_create = [
                "dbt",
                "run-operation", "create_table",
                "--args", json.dumps(args_create),
                "--profiles-dir", "./sqlcreator/.dbt",
                "--project-dir", "./sqlcreator"
            ]
            
            logger.debug(f"Executing DDL for table {table_name}.")
            result_create = subprocess.run(cmd_create, capture_output=True, text=True)

            if result_create.returncode != 0:
                logger.error(f"FAILURE: DBT create_table failed for {table_name}.")
                logger.error(f"STDOUT: {result_create.stdout.strip()}")
                logger.error(f"STDERR: {result_create.stderr.strip()}")
                continue
            
            logger.info(f"SUCCESS: Table {table_name} created successfully.")

            if add_info:
                try:
                    folder, csv_file = table_name.split("_", 1)
                    csv_path = Path(file_path_datasets) / folder / f"{csv_file}.csv"
                    
                except ValueError:
                    logger.error(f"Skipping data load for {table_name}: Invalid key format (expected folder_filename).")
                    continue

                if not csv_path.exists():
                    logger.warning(f"CSV file not found, skipping data load for {table_name}: {csv_path}")
                    continue

                insert_args = {
                    "file_path": str(csv_path.resolve()),
                    "table_path": f"bronze.{table_name}"
                }

                insert_cmd = [
                    "dbt", "run-operation", "inser_data",
                    "--args", json.dumps(insert_args),
                    "--profiles-dir", "./sqlcreator/.dbt",
                    "--project-dir", "./sqlcreator"
                ]
                
                logger.debug(f"Executing DML (insert data) for table {table_name}.")
                insert_result = subprocess.run(insert_cmd, capture_output=True, text=True)

                if insert_result.returncode != 0:
                    logger.error(f"FAILURE: DBT inser_data failed for {table_name}.")
                    logger.error(f"STDOUT: {insert_result.stdout.strip()}")
                    logger.error(f"STDERR: {insert_result.stderr.strip()}")
                else:
                    logger.info(f"SUCCESS: Data loaded into {table_name}.")
                    
    except Exception as e:
        logger.error(f"An unexpected error occurred during table processing: {e}", exc_info=True)
        return False
        
    return True

        
