import copy
import subprocess
from os import walk, path
from os.path import join, splitext
from json import load
from typing import Dict, Any, List, Tuple, Set
from concurrent.futures import ThreadPoolExecutor

from transform_generator.lib.dependency_analysis import identify_cyclic_dependencies, get_dependency_graph_for_configs
from transform_generator.lib.project_config_entry import ProjectConfigEntry
from transform_generator.lib.logging import get_logger
from transform_generator.lib.utils import to_be_deployed

logger = get_logger(__name__)
ACTIVITY_LIMIT = 40
ACTIVITY_WARNING = 35


def rename_adf_activities(activity_name: str) -> str:
    """
    This function takes in a string file or activity name and replaces
    characters to make it compliant with Azure Data Factory naming conventions
    :param activity_name: String of activity name to adjust
    :return: String of processed activity name
    """
    return activity_name.replace('.', '-')


def _get_virtualization_param_info(project_config: Dict[str, ProjectConfigEntry], var_type: str,
                                   default_value: bool = False, default_value_expression: bool = False) -> Dict[
        str, Dict[str, str]]:
    """
    This function uses the generator config and gets the virtualization parameters ready to be used in the ADF Pipeline.
    @param project_config: A dictionary with the config filename as the key and generator config entries as the values
    @param var_type: A string with what type the parameters are going to be (i.e. String or Expression)
    @param default_value: A boolean if a default value needs to be included in the parameter info
    @param default_value_expression: A boolean if there is an expression that needs to be included in the parameter info
    @return: A dictionary containing the parameter information for the ADF Pipeline
    """
    virtualization_params = {}
    for config_filename, project_config_entry in project_config.items():
        parallel_db_name = project_config_entry.parallel_db_name
        if project_config_entry.parallel_db_name != '':
            parallel_db_name += '_db'
            virtualization_params[parallel_db_name] = {
                'type': var_type
            }
            if default_value:
                virtualization_params[parallel_db_name]['defaultValue'] = parallel_db_name
            if default_value_expression:
                virtualization_params[parallel_db_name]['value'] = '@pipeline().parameters.' + parallel_db_name
    return virtualization_params


def _get_pipeline_parameters(params: Dict[str, Dict[str, str]], folder_location_type: str,
                             default_value: bool = False,
                             default_processing_date_expression: str = '',
                             default_processing_date_string: bool = False,
                             processing_date: bool = True) -> Dict[str, Dict[str, str]]:
    """
    This function takes in the database parameter information and creates the object that needs to be added to the ADF
    pipeline JSON.
    @param params: A dictionary with the parameter name as the key and the metadata about that parameter as a dictionary
        for the value
    @param folder_location_type: The type of the folder location, either 'String' or 'Expression'
    @param default_value: boolean if there is a default value for items like the folder_location
    @param default_processing_date_expression: string, empty if none, variable if the default value takes from the
        variable value, parameter if the default value takes from the parameter value
    @param default_processing_date_string: boolean if there is a default string value for the processing date
    @param processing_date: boolean if the processing date parameter is included in the output
    @return: A dictionary containing the parameter information for the ADF Pipeline JSON
    """
    pipeline_params = {}
    for param, param_metadata in params.items():
        pipeline_params[param] = {}
        for key, value in param_metadata.items():
            pipeline_params[param][key] = value
    if processing_date:
        pipeline_params['processing_date'] = {
            'type': folder_location_type
        }
        if default_processing_date_expression == 'variable':
            pipeline_params['processing_date']['value'] = '@variables(\'var_processing_date\')'
        elif default_processing_date_expression == 'parameter':
            pipeline_params['processing_date']['value'] = '@pipeline().parameters.processing_date'

        if default_processing_date_string:
            pipeline_params['processing_date']['defaultValue'] = 'yyyy-MM-dd'
    return pipeline_params


def _get_activity_cell(name: str, depends_on: Set[str], notebook_path: str,
                       activity_parameters: Dict[str, Dict[str, str]], cluster_name: str) -> Dict[str, Any]:
    """
    This creates and returns the JSON that represents the activities making up an ADF Pipeline.
    @param name: Name of the activity
    @param depends_on: The dependencies of the activity
    @param notebook_path: The path to the notebook's location in Databricks
    @param activity_parameters: The parameters that are passed into each activity
    @return: JSON of the ADF Pipeline activity cell
    """

    return {
        "name": name,
        "type": "DatabricksNotebook",
        "dependsOn": _get_activity_dependencies(depends_on) if len(depends_on) != 0 else [],
        "policy": {
            "timeout": "7.00:00:00",
            "retry": 2,
            "retryIntervalInSeconds": 180,
            "secureOutput": False,
            "secureInput": False
        }, "userProperties": [],
        "typeProperties": {
            "notebookPath": notebook_path,
            "baseParameters": activity_parameters
        },
        "linkedServiceName": {"referenceName": cluster_name, "type": "LinkedServiceReference"}
    }


def _create_notebook_path(activity_parameters, file_path, folder_location):
    notebook_path = folder_location + file_path
    return notebook_path


def get_notebook_paths_by_target_table(config_entries, filenames_by_target_table, folder_location, folder_path,
                                       database_variables):
    notebook_paths_by_target_table = {}
    for config_entry in config_entries:
        if config_entry.target_type not in {'view', 'lineage'}:
            if config_entry.target_table in filenames_by_target_table:
                notebook_path_end = folder_path + '/' + config_entry.target_table
                notebook_paths_by_target_table[config_entry.target_table] = _create_notebook_path(
                    database_variables, notebook_path_end, folder_location)
            else:
                notebook_paths_by_target_table[config_entry.target_table] = folder_location + config_entry.input_files[0]
    return notebook_paths_by_target_table


def _get_activity_dependencies(dependencies: Set[str]) -> List[Dict[str, Any]]:
    """
    This function will create and return the dependsOn structure for the pipeline
    :param dependencies: The necessary dependencies
    :return: JSON object
    """
    depends_on_container = []
    for dependency in dependencies:
        depends_on_shell = {"activity": dependency, "dependencyConditions": ["Succeeded"]}
        depends_on_container.append(depends_on_shell)
    return depends_on_container


def generate_adf_pipeline(pipeline_name: str, activities: List[Tuple[str, str, Set[str]]],
                          project_config: Dict[str, ProjectConfigEntry], cluster_name: str,
                          database_param_prefix: str = '', cluster_by_config_file_name: Dict[str, str] = {},
                          ddl_wrapper_by_config_file_name: Dict[str, str] = {}) -> Dict[str, Any]:
    """
    This function will construct a JSON ADF Pipeline for Azure DataFactory.

    @param pipeline_name: Name of the ADF Pipeline
    @param activities: A list of dictionaries that make up the notebooks
    @param project_config: A dictionary with the config filename as the key and generator config entries as the values
        required for the ADF Pipelines. Example of the minimum required values:
            activities = ('Activity1', [dependencies], 'Shared/DBNR_LATAM')
    @param database_param_prefix: String of the database parameterization prefix, if database_variables is false, it is
        an empty string.
    @return: JSON ADF Pipeline
    """

    pipeline_shell = generate_pipeline_shell(pipeline_name, project_config, database_param_prefix)

    if 'ddl' in pipeline_name.lower():
        activity_parameters = _get_pipeline_parameters(_get_virtualization_param_info(
            project_config, 'Expression', default_value_expression=True), 'Expression',
            processing_date=False)
    else:
        activity_parameters = _get_pipeline_parameters(_get_virtualization_param_info(
            project_config, 'Expression', default_value_expression=True), 'Expression',
            default_processing_date_expression='parameter')

    # Not using os.path.join() because this path is for Databricks and therefore we do not want the OS separator
    activity_count = 0
    for activity_name, filename, depends_on, notebook_path, exec_notebook in activities:
        if cluster_by_config_file_name and filename in cluster_by_config_file_name:
            cluster_name = cluster_by_config_file_name[filename]
        if 'ddl' in pipeline_name.lower() and ddl_wrapper_by_config_file_name and filename in ddl_wrapper_by_config_file_name:
            activity_parameters['exec_notebook'] = notebook_path
            notebook_path = exec_notebook
        else:
            if 'exec_notebook' in activity_parameters.keys():
                del activity_parameters['exec_notebook']
        activity_shell = _get_activity_cell(rename_adf_activities(activity_name), depends_on,
                                            notebook_path, copy.deepcopy(activity_parameters), cluster_name)

        pipeline_shell['properties']['activities'].append(activity_shell)
        activity_count += 1

    if activity_count > ACTIVITY_LIMIT:
        logger.error('Pipeline %s has %d activities, exceeding the activity limit set %d',
                     pipeline_name, activity_count, ACTIVITY_LIMIT)
    elif activity_count >= ACTIVITY_WARNING:
        logger.warning('Pipeline %s has %d activities, warning to approaching the activity limit set to %d',
                       pipeline_name, activity_count, ACTIVITY_LIMIT)

    return pipeline_shell


def generate_pipeline_shell(pipeline_name, project_config, database_param_prefix):
    pipeline_name = rename_adf_activities(pipeline_name)
    pipeline_shell = {
        "name": pipeline_name,
        "properties": {
            "activities": [],
            'parameters': {},
            "annotations": [],
            "folder": {"name": ''}
        },
        "type": "Microsoft.DataFactory/factories/pipelines"
    }

    if database_param_prefix:
        pipeline_shell['properties']['folder']['name'] = database_param_prefix
    elif 'ddl' in pipeline_name.lower():
        pipeline_shell['properties']['folder']['name'] = 'DDL'
    elif 'core' in pipeline_name.lower():
        pipeline_shell['properties']['folder']['name'] = 'CorePipelines'
    else:
        pipeline_shell['properties']['folder']['name'] = 'SemanticPipelines'

    if 'core_semantic_transformations' in pipeline_name.lower():
        pipeline_shell['properties']['parameters'] = _get_pipeline_parameters(_get_virtualization_param_info(
            project_config, 'String', default_value=True), 'String', default_value=True)
    elif 'ddl' in pipeline_name.lower():
        pipeline_shell['properties']['parameters'] = _get_pipeline_parameters(_get_virtualization_param_info(
            project_config, 'String', default_value=True), 'String', default_value=True, processing_date=False)
    else:
        pipeline_shell['properties']['parameters'] = _get_pipeline_parameters(_get_virtualization_param_info(
            project_config, 'String', default_value=False), 'String')

    return pipeline_shell


def generate_datafactory_pipeline(dependency_graph: Dict, notebook_paths_by_target_table: Dict[str, str],
                                  config_file_name: str, project_config: Dict[str, ProjectConfigEntry],
                                  cluster_name: str, database_param_prefix: str = '') -> Dict[str, Any]:
    """
    This function extracts the dependencies for each table and uses those values to call and return the ADF
    pipeline JSON.
    @param dependency_graph: A dictionary with the key being every table and the values being a tuple containing a set
        of the source tables and a set of the target tables that make up the table dependencies
    @param config_file_name: a string containing the config file name to make a unique name for the outputted ADF JSON.
    @param project_config: A dictionary with the config filename as the key and generator config entries as the values
    @param database_param_prefix: String of the database parameterization prefix, if database_variables is false, it is
        an empty string.
    @return: pipeline - A JSON that is the ADF pipeline for each notebook that needs to be called with the necessary
        dependencies. If there are no ADF activities, a pipeline will not be generated, and None will be returned.
    """
    cyclic_dependencies = identify_cyclic_dependencies(dependency_graph)
    for table in cyclic_dependencies:
        logger.error('Found cyclic dependency for %s', table)
    adf_parameters = []
    for tgt_table, notebook_path in notebook_paths_by_target_table.items():
        activity_name = rename_adf_activities(tgt_table)
        depends_on = set()
        for src_table in dependency_graph[tgt_table]:
            if src_table in notebook_paths_by_target_table:
                depends_on.add(rename_adf_activities(src_table))
        depends_on = sorted(depends_on)
        adf_parameters.append((activity_name, tgt_table, depends_on, notebook_path, ''))

    # generate the adf pipeline json
    if adf_parameters:
        adf_file_name = rename_adf_activities(splitext(config_file_name)[0].replace('config_', ''))
        pipeline = generate_adf_pipeline(adf_file_name, adf_parameters, project_config, cluster_name,
                                         database_param_prefix)
        return pipeline
    else:
        return None


def generate_ddl_datafactory_pipeline(activities: Set[str], pipeline_name: str, ddl_file_deps: Dict[str, Set[str]],
                                      folder_location: str, folder_path: str, database_variables: bool,
                                      project_config: Dict[str, ProjectConfigEntry],
                                      database_param_prefix: str = '') -> Dict[str, Any]:
    """
    This function extracts the dependencies for each table and uses those values to call and
    return the ADF pipeline JSON.
    @param activities: A set of strings containing the names of the config files referencing each DDL notebook.
    @param pipeline_name: A string containing the config file name to make a unique name for the outputted ADF JSON.
    @param ddl_file_deps: A dictionary of strings of the ddl views containing a set of the ddl file dependencies.
    @param folder_location: The Databricks activities folder name.
    @param project_config: A dictionary with the config filename as the key and generator config entries as the values
    @param database_param_prefix: String of the database parameterization prefix, if database_variables is false, it is
        an empty string.
    @return: pipeline - A JSON that is the ADF pipeline for each DDL notebook that needs to be called.
    """
    adf_parameters = []
    cluster_by_config_file_name = {}
    ddl_wrapper_by_config_file_name = {}
    for config_file_name in activities:
        if 'views' in config_file_name:
            group_name = project_config[config_file_name.replace('_views', '.csv')].group_name
            cluster = project_config[config_file_name.replace('_views', '.csv')].cluster_name
            ddl_wrapper = project_config[config_file_name.replace('_views', '.csv')].ddl_wrapper_script
        else:
            group_name = project_config[config_file_name + '.csv'].group_name
            cluster = project_config[config_file_name + '.csv'].cluster_name
            ddl_wrapper = project_config[config_file_name + '.csv'].ddl_wrapper_script

        if cluster:
            cluster_by_config_file_name[config_file_name] = cluster
        if ddl_wrapper:
            ddl_wrapper_by_config_file_name[config_file_name] = ddl_wrapper

        activity_name = rename_adf_activities(config_file_name)
        dependencies = ddl_file_deps.get(config_file_name, set())
        config_path = ''
        if group_name != '':
            config_path += '/' + group_name
        config_path += folder_path + '/' + config_file_name

        notebook_path = _create_notebook_path(database_variables, config_path, folder_location)

        exec_notebook = ''
        if ddl_wrapper:
            exec_notebook = _create_notebook_path(
                database_variables, '/' + group_name + ddl_wrapper, folder_location)

        adf_parameters.append((activity_name, config_file_name, dependencies, notebook_path, exec_notebook))

    # generate the adf pipeline json
    if database_variables:
        pipeline = generate_adf_pipeline(pipeline_name, adf_parameters, project_config, 'HighConCluster',
                                         database_param_prefix, cluster_by_config_file_name,
                                         ddl_wrapper_by_config_file_name)
    else:
        pipeline = generate_adf_pipeline(pipeline_name, adf_parameters, project_config, 'HighConCluster',
                                         cluster_by_config_file_name=cluster_by_config_file_name,
                                         ddl_wrapper_by_config_file_name=ddl_wrapper_by_config_file_name)

    return pipeline


def _create_pipeline_activity(activity_name: str, depends_on: List[Dict[str, Any]], reference_name: str,
                              project_config: Dict[str, ProjectConfigEntry],
                              main_pipeline: bool = False) -> Dict[str, Any]:
    pipeline = {
        "name": activity_name,
        "type": "ExecutePipeline",
        "dependsOn": depends_on,
        "userProperties": [],
        "typeProperties": {
            "pipeline": {
                "referenceName": reference_name,
                "type": "PipelineReference"
            },
            "waitOnCompletion": True
        }
    }
    if main_pipeline:
        pipeline['typeProperties']['parameters'] = _get_pipeline_parameters(_get_virtualization_param_info(
            project_config, 'Expression', default_value_expression=True), 'Expression',
            default_processing_date_expression='variable')
    else:
        pipeline['typeProperties']['baseParameters'] = _get_pipeline_parameters(_get_virtualization_param_info(
            project_config, 'Expression', default_value_expression=True), 'String',
            default_processing_date_expression='parameter')

    return pipeline


def _if_expression_pipeline_activity(activity: str, depend: Set[str], day: int, schedule_type: str,
                                     project_config: Dict[str, ProjectConfigEntry]) -> Dict[str, str]:
    """
    This function creates the activities for the if expression of an ADF pipeline.
    @param activity: A string of the activity name
    @param depend: A set of items the activity depends on
    @param day: An integer representing the day of the week that the activity will run.
    @param schedule_type: A string of the schedule type. Currently 'weekly' or 'monthly' only supported.
    @return: A dictionary containing the activity of the if expression that notes the day of the week the activity in
        the True expression will run.
    """
    if schedule_type == 'weekly':
        expression = '@equals(dayOfWeek(variables(\'var_processing_date\')), {})'.format(day)
    elif schedule_type == 'monthly':
        expression = '@equals(dayOfMonth(variables(\'var_processing_date\')), {})'.format(day)
    activity_dict = {
        "name": rename_adf_activities(activity),
        "type": "IfCondition",
        "dependsOn": _get_activity_dependencies(depend),
        "userProperties": [],
        "typeProperties": {
            "expression": {
                "value": expression,
                "type": "Expression"
            },
            "ifTrueActivities": [_create_pipeline_activity(rename_adf_activities(activity + '_if_true'), [],
                                                           rename_adf_activities(activity), project_config,
                                                           main_pipeline=True)]
        }
    }
    return activity_dict


def _set_processing_date_var_activity():
    return {
        'name': 'set_processing_date',
        'type': 'SetVariable',
        'dependsOn': [],
        'userProperties': [],
        'typeProperties': {
            'variableName': 'var_processing_date',
            'value': {
                'value': '@if(equals(pipeline().parameters.processing_date, \'yyyy-MM-dd\'),formatDateTime(convertTimeZone(utcnow(),\'UTC\',\'Eastern Standard Time\'), \'yyyy-MM-dd\'),trim(pipeline().parameters.processing_date))',
                'type': 'Expression'
            }
        }
    }


def _get_main_pipeline_activities(activities: Dict[str, Set[str]], project_config: Dict[str, ProjectConfigEntry],
                                  processing_date: bool) -> List[Dict[str, Any]]:
    """
    This function gets the activities for the main ADF pipeline
    @param activities: A dictionary with the activity name as the key and its dependencies as a set as the value
    @param project_config: A dictionary with the config filename as the key and generator config entries as the values
    @return: A List of dictionaries of the activities for the main ADF pipeline
    """
    weekday_lookup = {'sun': 0,
                      'mon': 1,
                      'tue': 2,
                      'wed': 3,
                      'thu': 4,
                      'fri': 5,
                      'sat': 6}

    container = []
    if processing_date:
        container.append(_set_processing_date_var_activity())

    for activity, depend in activities.items():
        sorted_depend = sorted(depend)
        if not sorted_depend and processing_date:
            sorted_depend = {'set_processing_date'}

        config_file_name = 'config_' + activity.replace('-', '.') + '.csv'
        if config_file_name in project_config and project_config[config_file_name].schedule_frequency == 'weekly':
            week_day = weekday_lookup[project_config[config_file_name].schedule_days.lower()]
            activity_dict = _if_expression_pipeline_activity(activity, sorted_depend, week_day, 'weekly',
                                                             project_config)
        elif config_file_name in project_config and project_config[config_file_name].schedule_frequency == 'monthly':
            month_day = int(project_config['config_' + activity + '.csv'].schedule_days)
            activity_dict = _if_expression_pipeline_activity(activity, sorted_depend, month_day, 'monthly',
                                                             project_config)
        else:
            activity_dict = _create_pipeline_activity(rename_adf_activities(activity),
                                                      _get_activity_dependencies(sorted_depend),
                                                      rename_adf_activities(activity),
                                                      project_config, main_pipeline=True)
        container.append(activity_dict)

    logger.info(' len(activity_dict) %d : len(container) %d ', len(activity_dict), len(container))

    activity_count = len(container)
    if activity_count > ACTIVITY_LIMIT:
        logger.error('Pipeline core_semantic_transformations has %d activities, exceeding the activity limit set %d',
                     activity_count, ACTIVITY_LIMIT)
    elif activity_count >= ACTIVITY_WARNING:
        logger.warning('Pipeline core_semantic_transformations has %d activities, '
                       'warning to approaching the activity limit set to %d',
                       activity_count, ACTIVITY_LIMIT)

    return container


def generate_main_adf_pipeline(pipeline_name: str, folder_name: str, activities_and_dependencies: Dict[str, Set[str]],
                               project_config: Dict[str, ProjectConfigEntry],
                               processing_date: bool = True) -> Dict[str, Any]:
    """
    This function generates the main ADF pipeline JSON.
    @param pipeline_name: String of the name of the pipeline being generated
    @param folder_name: String of the name of the folder where the pipeline lives in ADF
    @param activities_and_dependencies: A dictionary containing the activities and their dependencies.
    @param project_config: A dictionary with the config filename as the key and generator config entries as the values
    @param processing_date: bool of if processing date is included
    @return: A JSON of the main ADF pipeline.
    """
    cyclic_dependencies = identify_cyclic_dependencies(activities_and_dependencies)
    for table in cyclic_dependencies:
        logger.error('Found cyclic dependency for %s', table)
    main_pipeline = {
        'name': rename_adf_activities(pipeline_name),
        'properties': {
            'activities': _get_main_pipeline_activities(activities_and_dependencies, project_config, processing_date),
            'parameters': _get_pipeline_parameters(_get_virtualization_param_info(
                project_config, 'String', default_value=True), 'String', default_value=True,
                default_processing_date_string=True),
            'variables': {'var_processing_date': {'type': 'String'}},
            'folder': {"name": folder_name},
            'annotations': []
        },
        'type': 'Microsoft.DataFactory/factories/pipelines'
    }
    return main_pipeline


def create_cmd(datafactory_name: str, pipeline_name: str, resource_group_name: str, pipeline_path: str) -> List[str]:
    """
    This function creates the array to pass into subprocess.getstatusoutput
    @param datafactory_name: String of the name of the datafactory
    @param pipeline_name: String of the name of the ADF pipeline
    @param resource_group_name: String of the name of the resource group
    @param pipeline_path: String, path to the location of the ADF pipeline json
    @return: List of strings for the subprocess.getstatusoutput call
    """
    return ['az', 'datafactory', 'pipeline', 'create', '--factory-name', datafactory_name, '--pipeline-name',
            pipeline_name, '--resource-group', resource_group_name, '--pipeline', pipeline_path]


def upload_to_adf(pipeline_info: Dict[str, Tuple[str, Set[str]]], pipeline_dependency_order: List[Set[str]],
                  datafactory_name: str, resource_group_name: str, deployment_pipeline_filter: set,
                  deployment_exclude_pipeline_folder: set):
    """
    Uploads generated pipelines to ADF using the AZ CLI through subprocess.getstatusoutput calls
    @param pipeline_info: Dictionary with the key being pipeline names and values being tuples containing the path to
        the adf pipeline and the dependencies for that pipeline
    @param pipeline_dependency_order: A list of sets of strings containing a list of the pipeline names in the order
        they can be uploaded to ADF
    @param datafactory_name: String of the name of the datafactory
    @param resource_group_name: String of the name of the resource group
    @param deployment_pipeline_filter: pipelines to be deployed
    @param deployment_exclude_pipeline_folder: pipeline folders to be excluded
    """
    import_pipeline_params = {
        'datafactory_name': datafactory_name,
        'deployment_exclude_pipeline_folder': deployment_exclude_pipeline_folder,
        'deployment_pipeline_filter': deployment_pipeline_filter,
        'pipeline_info': pipeline_info,
        'resource_group_name': resource_group_name
    }
    with ThreadPoolExecutor() as tpe:
        for pipeline_set in pipeline_dependency_order:
            params = [(import_pipeline_params, pipeline_name) for pipeline_name in pipeline_set]
            tpe.map(import_pipeline_to_adf, params)


def import_pipeline_to_adf(import_pipeline_params):
    import_pipeline_parameters, pipeline_name = import_pipeline_params
    pipeline_path, _ = import_pipeline_parameters['pipeline_info'][pipeline_name]
    if to_be_deployed(import_pipeline_parameters['deployment_pipeline_filter'], pipeline_path) \
            and not to_be_deployed(import_pipeline_parameters['deployment_exclude_pipeline_folder'], pipeline_path):
        logger.info(pipeline_name + " being uploaded")
        pipeline_path = '@' + pipeline_path
        cmd = create_cmd(import_pipeline_parameters['datafactory_name'], pipeline_name,
                         import_pipeline_parameters['resource_group_name'], pipeline_path)
        cmd = ' '.join(cmd)
        logger.info(cmd)
        cmd_out = subprocess.run(cmd, capture_output=True, shell=True)
        if cmd_out.returncode != 0:
            logger.error('Unable to upload the pipeline %s : %s', pipeline_name, cmd_out.stderr)


def get_dependencies_from_pipeline(pipeline_element: Dict[str, Any]) -> Set[str]:
    """
    Extracts the pipeline dependencies from an ADF pipeline
    @param pipeline_element: A dictionary element from an ADF pipeline
    @return: A set of strings containing the pipeline dependencies from an ADF pipeline
    """
    result = set()
    for key, value in pipeline_element.items():
        if type(value) is list:
            for item in value:
                if type(item) is dict and len(item) > 0:
                    result = result | get_dependencies_from_pipeline(item)
        elif type(value) is dict and len(value) > 0:
            result = result | get_dependencies_from_pipeline(value)
        elif key == "type" and value == "PipelineReference":
            result.add(pipeline_element['referenceName'])
            return result

    return result


def get_pipelines(pipeline_path: str, ddl_only: bool = False) -> Dict[str, Tuple[str, Set[str]]]:
    """
    Validates a JSON as an ADF pipeline and creates a dictionary of pipeline names and path and dependencies
    @param pipeline_path: The path to the directory containing the ADF Pipelines
    @param ddl_only: boolean to indicate type of pipelines
    @return: A dictionary with the key being pipeline names and values being tuples containing the path to the adf
        pipeline and the dependencies for that pipeline
    """

    pipeline_info = {}
    for root, dirs, files in walk(pipeline_path):
        for file in files:
            if splitext(file.lower())[1] == '.json':

                if ddl_only and not (file.lower().startswith('ddl') or splitext(file.lower())[0].endswith('_ddl')):
                    continue

                filepath = join(root, file)

                pipeline_info = pipeline_info | get_pipeline_info(filepath)
                
    return pipeline_info


def get_pipeline_info(filepath: str) -> Dict[str, Tuple[str, Set[str]]]:
    valid_pipeline_keys = ['name', 'properties', 'type']
    valid_pipeline_type = 'Microsoft.DataFactory/factories/pipelines'

    with open(filepath) as f:
        payload = load(f)
        if list(payload.keys()) == valid_pipeline_keys and payload['type'] == valid_pipeline_type:
            dependencies = get_dependencies_from_pipeline(payload)
            pipeline_info = {payload['name']: (filepath, dependencies)}
        else:
            logger.error('%s is not a valid ADF Pipeline JSON', filepath)

    return pipeline_info or {}


def get_pipeline_dependency_order(pipeline_info: Dict[str, Tuple[str, Set[str]]]) -> List[Set[str]]:
    """
    Creates a list of sets of the correct order the ADF Pipelines can be uploaded
    @param pipeline_info: A dictionary with the key being pipeline names and values being tuples containing the path to
        the adf pipeline and the dependencies for that pipeline
    @return: A list of sets of the correct order the ADF Pipelines can be uploaded
    """
    processed_pipelines = set()
    pipeline_dependency_order = []
    missing_pipelines = set()

    while len(processed_pipelines) < len(pipeline_info):
        pipelines_to_add = set()
        for key, value in pipeline_info.items():
            file_path, dependencies = value
            for dependency in dependencies:
                if dependency not in pipeline_info and dependency not in missing_pipelines:
                    missing_pipelines.add(dependency)

            if dependencies - processed_pipelines - missing_pipelines == set() and key not in processed_pipelines:
                pipelines_to_add.add(key)
        pipeline_dependency_order.append(pipelines_to_add)
        processed_pipelines = processed_pipelines | pipelines_to_add

    if missing_pipelines:
        logger.error('These dependent pipelines are not found in this set of repos to deploy: %s', missing_pipelines)

    return pipeline_dependency_order


def generate_module_adf_pipeline(config_filename_by_target_table, configs_by_config_filename, database_param_prefix,
                                 database_variables, project_config, group_name, mappings_by_mapping_filename,
                                 output_datafactory, modules):
    """
    This function generates the module-level ADF pipeline JSON.
    @param config_filename_by_target_table: A dictionary with target table as the key and config filename as the values
    @param configs_by_config_filename: A dictionary where the key is a config filename string and the values are lists
        of config entries.
    @param database_param_prefix: A string containing the prefix for the ADF Pipelines if using database variables
    @param database_variables: A boolean indicating if database variables is active
    @param project_config: A dictionary with the config filename as the key and generator config entries as the values
    @param group_name: A string containing the name of the module
    @param mappings_by_mapping_filename: A dictionary with a key of mapping filename and value of the DataMapping
    @param output_datafactory: A string containing the path to the datafactory output folder
    @return: A JSON of the module-level ADF pipeline.
    """
    module_pipeline_name = group_name + '_transformations'
    module_pipeline_folder = group_name
    if database_variables:
        module_pipeline_name = database_param_prefix + '_' + module_pipeline_name
        module_pipeline_folder = database_param_prefix
    output_adf_dir = path.join(output_datafactory, 'datafactory', 'pipelines', 'generated')

    config_dependency_graph = get_dependency_graph_for_configs(configs_by_config_filename, mappings_by_mapping_filename,
                                                               config_filename_by_target_table, project_config,
                                                               modules)
    main_pipeline = generate_main_adf_pipeline(module_pipeline_name, module_pipeline_folder,
                                               config_dependency_graph, project_config)
    module_pipeline_path = path.join(output_adf_dir, module_pipeline_name + '.json')
    return main_pipeline, module_pipeline_path
