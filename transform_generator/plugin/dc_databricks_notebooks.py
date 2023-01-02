import re
from typing import Optional

from transform_generator.lib.data_mapping import DataMapping
from transform_generator.lib.project_config_entry import ProjectConfigEntry
from transform_generator.plugin.generate_databricks_notebooks import GenerateTransformationNotebooks, \
    GenerateTableViewCreateNotebooks, GenerateDatabricksNotebooksStage
from transform_generator.project import Project


class GenerateDcNotebooksAction(GenerateDatabricksNotebooksStage):
    def __init__(self, name: str, widget_base_path: str = ""):
        super().__init__(name)
        self._parallel_db_names_by_db_table_name = None
        self._widget_base_path = widget_base_path

    def _notebook_prologue(self,
                           data_mappings: list[DataMapping],
                           mapping_group_config: ProjectConfigEntry) -> Optional[str]:
        prologue = _dc_notebook_prologue(self._widget_base_path,
                                         mapping_group_config,
                                         self._parallel_db_names_by_db_table_name)
        if prologue:
            self._write(prologue)
            self._end_cell()

    def _notebook_epilogue(self,
                           data_mappings: list[DataMapping],
                           mapping_group_config: ProjectConfigEntry):
        db, table_name = self.get_db_table_name(data_mappings[0].table_name_qualified)
        epilogue = _dc_notebook_epilogue(data_mappings, mapping_group_config, db, table_name)
        if epilogue:
            self._write(epilogue)

    def execute(self, projects: list[Project], /, *, target_output_dir: str, **kwargs):
        self._parallel_db_names_by_db_table_name = {m.table_name_qualified: g.mapping_group_config.parallel_db_name
                                                    for p in projects
                                                    for g in p.data_mapping_groups
                                                    for m in g.data_mappings
                                                    if g.mapping_group_config.parallel_db_name}
        super().execute(projects, target_output_dir=target_output_dir, **kwargs)


class GenerateDcTransformationNotebooks(GenerateDcNotebooksAction, GenerateTransformationNotebooks):
    pass


class GenerateDcTableViewNotebooks(GenerateDcNotebooksAction, GenerateTableViewCreateNotebooks):
    pass


def _dc_notebook_prologue(widget_base_path: str,
                          mapping_group_config: ProjectConfigEntry,
                          _parallel_db_names_by_db_table_name: dict[str, str]) -> str:
    prologue = "CREATE WIDGET TEXT processing_date DEFAULT '';\n"
    prologue += _base_path_widget(widget_base_path)

    if mapping_group_config.notebooks_to_execute:
        if prologue:
            prologue += '\n'
        prologue += '%run\n' + '\n'.join(mapping_group_config.notebooks_to_execute.split(','))

    if _parallel_db_names_by_db_table_name:
        if prologue:
            prologue += '\n'
        prologue += create_db_param_widgets()
    return prologue


def _base_path_widget(widget_base_path) -> str:
    base_path_widget_text = ''
    if widget_base_path:
        raw_base_path_parameters = re.findall(r'\${[^}]*}', widget_base_path)
        base_path_widgets = []
        for parameter in raw_base_path_parameters:
            param = parameter.replace('${', '').replace('}', '')
            widget = 'CREATE WIDGET TEXT ' + param.split('.')[0] + ' DEFAULT \'\';'
            base_path_widgets.append(widget)
        base_path_widget_text = '\n'.join(base_path_widgets)
    return base_path_widget_text


def create_db_param_widgets(parallel_dbs: list[str]):
    widgets = set()

    for parallel_db_name in parallel_dbs:
        widgets.add('CREATE WIDGET TEXT ' + parallel_db_name + '_db DEFAULT \'' + parallel_db_name + '\';')

    return '\n'.join(widgets)


def _dc_notebook_epilogue(data_mappings: list[DataMapping],
                          mapping_group_config: ProjectConfigEntry,
                          db: str,
                          table_name: str) -> str:
    load_synapse = any(True for d in data_mappings if d.config_entry.load_synapse == 'Y')
    folder_path = ''
    if mapping_group_config.group_name:
        folder_path += '/' + mapping_group_config.group_name

    if load_synapse:
        return f'%run {folder_path}/cons_digital_core/databricks/synapse/syn_load ' \
               f'$src_table = "{table_name}" $tgt_table = "{table_name}" $tgt_schema = ' \
               f'{db}'
