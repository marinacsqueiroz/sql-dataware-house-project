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
from ydata_profiling import ProfileReport

base_path = "datasets"

dataframes = {}

file_path_config = os.path.join("scripts", "config", "column_type_config.json")

with open(file_path_config, "r", encoding="utf-8") as f:
    column_type = json.load(f)

for root, dirs, files in os.walk(base_path):
    for file in files:
        if file.lower().endswith(".csv"):
            file_path = os.path.join(root, file)
            relative_path = os.path.relpath(file_path, base_path)
            df_name = (relative_path.replace("\\", "/").replace("/", "_").replace(".csv", "")).lower()
            
            df = pd.read_csv(file_path)

            df_dtypes = {col.lower(): dtype for col, dtype in df.dtypes.to_dict().items()}

            for col, new_type in column_type.items():
                for column in df_dtypes.keys():
                    if col in column:
                        df_dtypes[column] = new_type
                    elif col in str(df_dtypes[column]):
                        df_dtypes[column] = new_type

            dataframes[df_name] = df_dtypes      

            profile = ProfileReport(df, title="Profiling Report")
            profile.to_file(f"./profile_report/analysis_html/{df_name}.html")

output_path = os.path.join("sqlcreator", "macros", "config", "column_types.json")
os.makedirs(os.path.dirname(output_path), exist_ok=True)

with open(output_path, "w", encoding="utf-8") as f:
    json.dump(dataframes, f, indent=4, ensure_ascii=False)

        