import os

from create_schema_and_models import create_schema_and_models
from data_profile import analyse_dataset


base_path = "datasets"
file_path_inicial_project = os.path.join("scripts", "config", "schema.json")
file_path_data_config = os.path.join("scripts", "config", "column_type_config.json")

schema_names = create_schema_and_models(file_path_inicial_project)
# analyse_dataset(base_path, file_path_data_config)

