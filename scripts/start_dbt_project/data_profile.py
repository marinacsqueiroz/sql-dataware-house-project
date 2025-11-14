# -----------------------------------------------------------------------------
# This script automates the process of scanning all CSV files inside the
# "datasets" directory, generating profiling reports, mapping data types,
# and exporting a consolidated configuration file in JSON format.
#
# Step-by-step:
# 1. Loads a JSON configuration file ("column_type_config.json") that defines
#    desired SQL or standardized data types for specific column names or
#    Pandas dtypes.
# 2. Recursively walks through every subfolder under "datasets/".
# 3. For each CSV file found:
#      - Builds a lowercase, underscore-separated name for the dataset.
#      - Reads the file into a pandas DataFrame.
#      - Converts all column names to lowercase and builds a dictionary of
#        {column_name: detected_dtype}.
#      - Compares each column and its type with the configuration file:
#          * If the column name or dtype matches a key in column_type_config,
#            its mapped type is applied to the output dictionary.
#      - Stores the mapped types in the "dataframes" dictionary.
#      - Generates an HTML profiling report using ydata_profiling and saves it
#        to "./profile_report/analysis_html/".
# 4. After processing all CSVs, writes the complete dictionary of inferred and
#    standardized column types to:
#       "./sqlcreator/macros/config/column_types.json"
#    This file can later be used by dbt macros to dynamically create tables
#    or apply schema definitions.
#
# Result:
# - An HTML profiling report per dataset (for data quality and exploration).
# - A JSON file summarizing column names and standardized types, ready for
#   integration with dbt or SQL schema generation workflows.
# -----------------------------------------------------------------------------

import json
import pandas as pd
import os
from logging import Logger
from ydata_profiling import ProfileReport

def analyse_dataset(base_path: str, file_path_config: str, logger: Logger, output_path: str) -> bool:
    
    dataframes = {}

    logger.info(f"Loading custom column type configuration from: {file_path_config}")
    try:
        try:
            with open(file_path_config, "r", encoding="utf-8") as f:
                column_type = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.error(f"FATAL: Failed to load or decode custom configuration file {file_path_config}: {e}", exc_info=True)
            return False

        logger.info(f"Starting recursive scan and analysis in directory: {base_path}")

        for root, _, files in os.walk(base_path):
            for file in files:
                if file.lower().endswith(".csv"):
                    file_path = os.path.join(root, file)
                    relative_path = os.path.relpath(file_path, base_path)
                    df_name = (relative_path.replace("\\", "/").replace("/", "_").replace(".csv", "")).lower()
                    
                    logger.info(f"Processing dataset: {df_name} from {file_path}")

                    df = pd.read_csv(file_path)

                    df_dtypes = {col.lower(): dtype for col, dtype in df.dtypes.to_dict().items()}

                    for col, new_type in column_type.items():
                        for column in df_dtypes.keys():
                            if col in column:
                                logger.debug(f"Mapped column '{column}' based on name match '{col}' to type '{new_type}'.")
                                df_dtypes[column] = new_type
                            elif col in str(df_dtypes[column]):
                                logger.debug(f"Mapped column '{column}' based on dtype match '{col}' to type '{new_type}'.")
                                df_dtypes[column] = new_type

                    dataframes[df_name] = df_dtypes      

                    # profile = ProfileReport(df, title="Profiling Report")
                    # report_path = (f"./profile_report/analysis_html/{df_name}.html")
                    # profile.to_file(report_path)
                    # logger.debug(f"Profiling report generated at: {report_path}")
        
        try:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(dataframes, f, indent=4, ensure_ascii=False)
            logger.info(f"SUCCESS: Final column type configuration saved to: {output_path}")
            return True
        except Exception as e:
            logger.error(f"FATAL: Failed to write final configuration file to {output_path}: {e}", exc_info=True)
            return False
        
    except Exception as e:
        logger.error(f"An unexpected error occurred during dataset scanning/profiling: {e}", exc_info=True)
        return False
