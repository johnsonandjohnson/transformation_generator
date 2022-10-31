# Digital Transformation - Metrics Automation
## Description
This repository performs the following:
- Generates bash scripts and sql queries from excel and csv mapping documents (generate_sql_output.py)
- Converts mapping documents from spreadsheets to csv and update config file with updated csv file name when applicable (mapping_converter.py)
- Generate ddl scripts based on config and schema files (generate_ddl_output.py)

## Project Setup

### System Prerequisites

For this project, Python 3.9+ is required. To ensure the proper distribution of Python is installed, use the version command

```sh
$ python --version
```

> NOTE: MacOS ships with Python 2 by default as the `python` command, and will typically also include a Python 3 distribution available as the command `python3`. If this is the case on your machine, use `python3` instead for all setup steps.

### Hosting options
There are several ways to host this project, including:
   * Within an IDE, such as PyCharm
   * In a Python isolation environment, such as virtualenv
   * In a Docker container

### Setup Steps
1. Clone this repository to the target build machine.
2. Create a virtual environment for the project then activate it.

   ##### Mac / Linux

   ```sh
   $ python -m venv venv

   $ . venv/bin/activate
   ```
   ##### Windows

   ```sh
   $ python -m venv venv

   $ .\venv\Scripts\activate.bat
   ```

3. Use pip to install all project dependencies into the virtual environment

   ```sh
   $ pip install -r requirements.txt
   ```

4. Run the unit tests to verify the install

   ```sh
   $ python -m unittest
   ```

## Application Use Cases

### Generate SQL Output

In order to generate SQL, run the `transform_generator.generate_sql_output` module from the root of this project.

Example files are included in this project's test directory, and can be used to quickly test SQL Generation. The following command will utilize the relative paths of these files and writes all output to an output folder at the project root.

#### Example Usage
```sh
$ python -m transform_generator.generate_sql_output --config_path test/Resources/positive_cases/config --schema_path test/Resources/positive_cases/schema --mapping_sheet_path test/Resources/positive_cases/mapping --project_config_path test/Resources/positive_cases/project_config/project_config_test.csv --output_datafactory output/datafactory --output_databricks output/databricks
```

#### Options and Arguments
   - `--config_path`: path to the config directory for the transform generator.
   - `--schema_path`: path to the directory containing .csv files for schemas
   - `--mapping_sheet_path`: path to the directory containing mapping sheets
   - `--project_config_path` semicolon delimited list to paths of the project config files
   - `--output_databricks`: path to the folder where databricks output is written. If folders in this path do not exist, they will be created.
   - `--output_datafactory`: path to the folder where databricks output is written. If folders in this path do not exist, they will be created.

### Generate DDL Output

#### Example Usage
```sh
$ python -m transform_generator.generate_ddl_output --config_path test/Resources/positive_cases/config --schema_path test/Resources/positive_cases/schema --mapping_sheet_path test/Resources/positive_cases/mapping --project_config_path test/Resources/positive_cases/project_config/project_config_test.csv --output_datafactory output/datafactory --output_databricks output/databricks
```

#### Options and Arguments
   - `--config_path`: path to the config directory for the transform generator.
   - `--schema_path`: path to the directory containing .csv files for schemas
   - `--mapping_sheet_path`: path to the directory containing mapping sheets
   - `--project_config_path` semicolon delimited list to paths of the project config files
   - `--output_databricks`: path to the folder where databricks output is written. If folders in this path do not exist, they will be created.
   - `--output_datafactory`: path to the folder where databricks output is written. If folders in this path do not exist, they will be created.

### Documentation

> Running the Documentation mandates a virtual environment activated with the requirements.txt file installed. This is accomplished as a through the completion of the first three [Setup Steps](#setup-steps).

Transformation Generator uses the static site generator [MkDocs](https://www.mkdocs.org/getting-started/#getting-started-with-mkdocs) to build project documentation. Once The project has been set up in a virtual environment, the documentation can be built for local development by:

1. Navigating to the `documentation` directory

   ```sh
   $ cd ./documentation
   ```

2. Starting the development server

    ```sh
    $ mkdocs serve
    ```

All documentation source files are contained within the `documentation/docs` directory and are written in Markdown. By default, the built site will be available at http://localhost:8000/

### Generating unit test coverage file

Generating an unit test coverage file or updating a pre-existing one can be done with following steps:

   1. Activate the virtual environment created during setup

      ```sh
      $ . venv/bin/activate
      ```

   2. Run the following commands to clear and generate new code coverage analysis:

      ```sh
      $ coverage erase
      $ coverage 
      ```
      
   3. View the report
      ```sh
      $ coverage report
      ```
      
### Running the Web Server

> Running the webserver mandates a virtual environment activated with the requirements.txt file installed. This is accomplished as a through the completion of the first three [Setup Steps](#setup-steps).

This application provides an API which can be hosted locally. To run it, at the root of the project run the module `transform_generator.api.api` from within a virtual environment.

```sh
$ python -m transform_generator.api.api
```

The API will start listening on port 8001 by default.
#### Accessing Swagger Documentation

Swagger documentation outlining the various endpoints offered by the API can be accessed from `/docs`