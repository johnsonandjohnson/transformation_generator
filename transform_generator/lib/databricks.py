from typing import Dict
from transform_generator.lib.logging import get_logger
from transform_generator.lib.project_config_entry import ProjectConfigEntry

logger = get_logger(__name__)


def create_sql_widget(project_config: Dict[str, ProjectConfigEntry]):
    """
    This function takes in the generator config file, loops through it, and extracts the variables that need to be
    created as widgets for the database variables.
    @param project_config: A dictionary with the config filename as the key and generator config entries as the values
    @return: a string containing the widget text for the beginning of the sql files
    """
    widgets = set()
    widgets.add('CREATE WIDGET TEXT processing_date DEFAULT \'\';')
    for config_filename, project_config_entry in project_config.items():
        parallel_db_name = project_config_entry.parallel_db_name
        if project_config_entry.parallel_db_name != '':
            widgets.add('CREATE WIDGET TEXT ' + parallel_db_name + '_db DEFAULT \'' + parallel_db_name + '\';')
    widget_string = '\n'.join(widgets)
    return widget_string
