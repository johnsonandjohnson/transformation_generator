# DDL Generator 

`generate_ddl_output.py`
usage:
```
python generate_ddl_output.py [--config_path] config_path [--schema_path] schema_path [--mapping_sheet_path] mapping_sheet_path [--output_databricks] output_databricks [--output_datafactory] output_datafactory [--databricks_folder_location] databricks_folder_location [--database_param_prefix] database_param_prefix [--project_config_path] project_config_path

Options and arguments
--config_path: Path to config directory containing a list of target tables that will be generated.
--schema_path: Path to directory containing CSV Schema files describing table structure
--mapping_sheet_path: Path to base directory containing data mapping sheets
--output_databricks: Base output directory for SQL files and Python scripts
--output_datafactory: Base output directory for ADF pipelines
--databricks_folder_location: default='cicd', Databricks folder location
--database_param_prefix: default='parameterization', string prefix for the ADF Pipelines
--project_config_path: Semicolon delimited list to paths ofthe project config files
```