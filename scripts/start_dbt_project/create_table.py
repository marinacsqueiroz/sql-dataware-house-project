



import json
import os
from pathlib import Path
import subprocess

import json
import os
import subprocess
from pathlib import Path
from logging import Logger

import yaml

def create_table(file_path_data_config: str, file_path_datasets: str, logger: Logger, schema: str, add_info: bool = False) -> bool:
    
    logger.info(f"Starting table creation and data loading using config: {file_path_data_config}")
    tables = []
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
                "schema": schema,
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

            
            dbt_stg_path = os.path.join("sqlcreator", "models", "staging", schema)
            sql_file_path = Path(dbt_stg_path) / Path(f"stg_{table_name}.sql")

            sql_content = (
                f"SELECT\n"
                f"    *\n"
                f"FROM {{{{ source('{schema}', '{table_name}') }}}}\n"
            )

            
            with open(sql_file_path, "w", encoding="utf-8") as f:
                f.write(sql_content)

            logger.info(f"STG model created: {sql_file_path}")

            table_entry = {
                "name": table_name,
                "description": f"Tabela origem {schema}.{table_name}",
                "columns": []
            }

            for col_name in item.keys():
                col_entry = {
                    "name": col_name,
                    "description": ""
                }

                table_entry["columns"].append(col_entry)

            tables.append(table_entry)

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
                    "table_path": f"{schema}.{table_name}"
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

        sources_yml = {
            "sources": [
                {
                    "name": schema,
                    "database": "DataWarehouse",
                    "schema": schema,
                    "tables": tables
                }
            ]
        }


        yml_path = Path(dbt_stg_path) / f"_src_{schema}.yml"

        with open(yml_path, "w", encoding="utf-8") as f:
            yaml.safe_dump(
                sources_yml,
                f,
                sort_keys=False,
                allow_unicode=True
            )

        logger.info(f"YAML file created: {yml_path}")      

    except Exception as e:
        logger.error(f"An unexpected error occurred during table processing: {e}", exc_info=True)
        return False
        
    return True

        
