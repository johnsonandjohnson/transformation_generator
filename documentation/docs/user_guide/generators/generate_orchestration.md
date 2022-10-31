# Orchestration Pipeline Generator 

`generate_orchestration.py`
usage:
```
python generate_orchestration.py [--repository_dir] repository_dir [----orchestration_dir] --orchestration_dir [--dependent_repo_names] dependent_repo_names [--odatabricks_folder_location] databricks_folder_location [--atabase_param_prefix] database_param_prefix

Options and arguments
--repository_dir: Path to local repos directory
--orchestration_dir: Path to orchestration directory
--dependent_repo_names: List of dependent repositories
--databricks_folder_location: default='cicd', Databricks folder location
--database_param_prefix: default='parameterization', String prefix for the ADF Pipelines
```