from transform_generator.parser.ast.select_query import SelectQuery
from transform_generator.parser.ast.from_clause import FromClause
from transform_generator.parser.ast.bin_op import BinOp


class SqlQueryGenerator:
    def __init__(self):
        self._output_str = ""

    def _emit(self, string):
        self._output_str += string

    def _indent(self, level):
        self._emit("\t" * level)

    def _print_result_columns(self, result_columns):
        self._emit(str(result_columns.pop(0)))
        for col in result_columns:
            self._emit(",\n")
            self._indent(1)
            self._emit(str(col))

    def _print_from_clause(self, from_clause: FromClause):
        self._emit("\nFROM ")
        if from_clause.database_name is not None:
            self._emit(from_clause.database_name + '.')
        self._emit(from_clause.table_name)

        if from_clause.alias is not None:
            self._emit(" AS " + from_clause.alias)

        for join in from_clause.joins:
            self._emit("\n")
            self._indent(1)
            self._emit(str(join))

    def _print_where_clause(self, where_clause: BinOp):
        if where_clause is not None:
            self._emit("\nWHERE ")
            self._emit(str(where_clause))

    def _print_group_by_clause(self, group_by_clause):
        if group_by_clause is not None:
            self._emit("\n")
            self._emit(str(group_by_clause))

    def print_select(self, select_query: SelectQuery):
        self._emit('SELECT ')
        if select_query.distinct:
            self._emit('DISTINCT ')
        self._print_result_columns(select_query.result_columns)
        self._print_from_clause(select_query.from_clause)
        self._print_where_clause(select_query.where_clause)
        self._print_group_by_clause(select_query.group_by_clause)

        return self._output_str



