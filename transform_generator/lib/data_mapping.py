from abc import ABC
from dataclasses import dataclass, field
from typing import Optional, Set

from transform_generator.lib.config_entry import ConfigEntry
from transform_generator.lib.table_definition import TableDefinition
from transform_generator.parser.ast.transform_exp import TransformExp


@dataclass(order=True)
class BaseMapping(ABC):
    key: str
    database_name: str
    table_name: str
    config_entry: ConfigEntry
    table_definition: TableDefinition

    @property
    def table_name_qualified(self) -> Set[str]:
        return self.database_name + "." + self.table_name


@dataclass(order=True)
class DataMapping(BaseMapping):
    target_column_names: set[str]
    ast_by_target_column_name: dict[str, TransformExp]
    comment_by_target_column_name: dict[str, Optional[str]] = field(default_factory=dict)

    def __post_init__(self):
        # Dependencies can be reliably inferred from one on the from_clauses,
        # since all target columns are from the same source tables in a given data mapping.
        self.table_dependencies = self.ast_by_target_column_name[
            list(self.target_column_names)[0]].from_clause.get_table_dependencies()

    def ast(self, target_column_name: str) -> TransformExp:
        """
        Returns the TransformationExp Abstract Syntax Tree for a given target column.
        param target_column_name: The names of the target column
        return: The TransformExp that represents the business rule for the given column.
        """
        return self.ast_by_target_column_name[target_column_name]


@dataclass(order=True)
class ProgramMapping(BaseMapping):
    def __post_init__(self):
        if self.config_entry:
            self.table_dependencies = self.config_entry.dependencies.split(',')
    pass
