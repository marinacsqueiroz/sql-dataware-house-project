# -----------------------------------------------------------------------------
# This script automatically searches for all CSV files inside the "datasets"
# folder (including its subfolders), loads each file into a pandas DataFrame,
# and generates an automated data profiling report for each CSV.
#
# Step-by-step:
# 1. It uses os.walk() to recursively traverse the "datasets" directory.
# 2. For every file ending with ".csv", it:
#    - Builds the full file path.
#    - Creates a clean name for the DataFrame/report based on the relative path.
#    - Reads the CSV file into a pandas DataFrame.
#    - Uses ydata_profiling (formerly pandas_profiling) to generate an HTML
#      profiling report summarizing the dataset’s structure, statistics, and
#      data quality (distributions, correlations, missing values, etc.).
# 3. Each report is saved as an HTML file under the folder "profile_report/",
#    with a file name matching the dataset path (e.g., source_crm.cust_info.html).
#
# Result:
# You get one detailed HTML report per CSV file — useful for quickly
# understanding data quality and characteristics in multiple source folders.
# -----------------------------------------------------------------------------

import pandas as pd
import os
from ydata_profiling import ProfileReport

base_path = "datasets"

dataframes = {}

for root, dirs, files in os.walk(base_path):
    for file in files:
        if file.lower().endswith(".csv"):
            file_path = os.path.join(root, file)
            relative_path = os.path.relpath(file_path, base_path)
            df_name = relative_path.replace("\\", "/").replace("/", ".").replace(".csv", "")
            
            df = pd.read_csv(file_path)
            
            profile = ProfileReport(df, title="Profiling Report")
            profile.to_file(f"./profile_report/{df_name}.html")


