# -----------------------------------------------------------------------------
# This script orchestrates the initial setup, analysis, and data modeling
# steps for a data project. It relies on a custom LogManager for robust
# logging and handles errors for each critical function.
#
# Process Overview:
# 1. Path Configuration: Defines standard paths for datasets and configuration files.
# 2. Logger Initialization: Sets up the LogManager to ensure all steps are
#    logged to a daily file with full traceback on errors.
# 3. Schema and Model Creation: Calls 'create_schema_and_models' using an
#    initial configuration file (e.g., schema.json).
# 4. Data Analysis and Profiling: Executes 'analyse_dataset' to scan the
#    'datasets' directory, infer types, and generate a standardized
#    'column_types.json' configuration file for downstream use.
# 5. Table Creation: Uses the newly generated configuration to call
#    'create_table', typically performing DDL (Data Definition Language)
#    operations in the target database.
# 6. Finalization: Ensures all buffered log messages are written to disk.
#
# Robustness: Each critical step is wrapped in a try...except block to log
# CRITICAL ERRORS with full traceback (exc_info=True), preventing silent failures.
# -----------------------------------------------------------------------------
import json
import os
import time
from create_schema_and_models import create_schema_and_models
from data_profile import analyse_dataset
from log import LogManager
from create_table import create_table

main_config_file_path = os.path.join("scripts", "start_dbt_project", "config", "main_config.json")
initial_project_file_path = os.path.join("scripts", "start_dbt_project", "config", "schema.json")
create_table_config_file_path = os.path.join("scripts", "start_dbt_project", "config", "column_type_config.json")
data_config_file_path = os.path.join("scripts", "start_dbt_project", "config", "column_types.json")


log_manager = LogManager(subdirectory='start_project')
logger = log_manager.get_logger()

logger.info("--- STARTING MAIN PROJECT CREATION AND ANALYSIS PROCESS ---")
bach_start_time = time.time()

try:
    with open(main_config_file_path, "r", encoding="utf-8") as f:
        main_config = json.load(f)
        base_path = main_config['base_path']
        raw_schema = main_config['raw_schema']
        insert_info = main_config['insert_info']
        database = main_config['database']
except (FileNotFoundError, json.JSONDecodeError) as e:
    logger.error(f"FATAL: Failed to load or decode custom configuration file {main_config_file_path}: {e}", exc_info=True)

logger.info(f"Starting creation of schemas and models from: {initial_project_file_path}")
datasets_file_path = os.path.join(base_path)
try:
    start_time = time.time()
    result_schema_and_models = create_schema_and_models(initial_project_file_path, logger=logger)
    
    if result_schema_and_models:
        end_time = time.time()
        duration = end_time-start_time
        logger.info(f"Schema and model creation successfully completed in {duration:.4f} seconds.")
    
    else:
        logger.warning("Schema and model creation completed, but returned a neutral/false status.")

except Exception as e:
    logger.error(f"CRITICAL ERROR during schema and model creation: {e}", exc_info=True)

logger.info(f"Starting dataset analysis (profiling) in: {base_path}")
try:
    start_time = time.time()
    result_analyse_dataset = analyse_dataset(base_path, create_table_config_file_path, logger=logger, output_path=data_config_file_path)
    end_time = time.time()
    duration = end_time-start_time
    logger.info(f"Dataset analysis completed. Configurations saved to: {data_config_file_path} in {duration:.4f} seconds")
    
except Exception as e:
    logger.error(f"CRITICAL ERROR during dataset analysis and configuration generation: {e}", exc_info=True)

logger.info(f"Starting table creation using config: {data_config_file_path}")
try:
    start_time = time.time()
    result_create_table = create_table(data_config_file_path, datasets_file_path, logger=logger, schema=raw_schema, database=database, add_info=insert_info)
    end_time = time.time()
    duration = end_time-start_time
    logger.info(f"Table (or DDL/DML model) creation successfully completed in {duration:.4f} seconds.")

except Exception as e:
    logger.error(f"CRITICAL ERROR during table creation: {e}", exc_info=True)

bach_end_time = time.time()
bach_duration = bach_end_time-bach_start_time
logger.info(f"--- MAIN PROCESS CONCLUDED IN {bach_duration:.4f} SECONDS---")
log_manager.flush_and_close()