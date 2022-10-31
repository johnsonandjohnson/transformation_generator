from typing import Set


class FromClause:
    def __init__(self, table_name, joins=[], database_name=None, alias=None):
        def qualified_tbl_name(db_name: str, tbl_name: str):
            return (db_name + "." if db_name is not None else "") + tbl_name
        self._table_name = table_name
        self._joins = joins if joins is not None else []
        self._database_name = database_name
        self._alias = alias

        db_tbl_name = qualified_tbl_name(database_name, table_name)
        tbl_dep_set = {db_tbl_name}
        alias_table_map = {table_name: db_tbl_name}
        if alias is not None:
            alias_table_map[alias] = db_tbl_name

        for join in self.joins:
            db_tbl_name = qualified_tbl_name(join.database_name, join.table_name)
            tbl_dep_set.add(db_tbl_name)
            alias_table_map[join.table_name] = db_tbl_name
            if join.alias is not None:
                alias_table_map[join.alias] = db_tbl_name

        self._table_dependencies = tbl_dep_set
        self._alias_table_name_map = alias_table_map

    @property
    def table_name(self):
        return self._table_name

    @property
    def joins(self):
        return self._joins

    @property
    def database_name(self):
        return self._database_name

    @property
    def alias_table_name_map(self):
        return self._alias_table_name_map

    @property
    def alias(self):
        return self._alias

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            database_match = self.database_name is None and other.database_name is None \
                             or self.database_name == other.database_name
            return self.table_name == other.table_name and self.joins == other.joins and database_match \
                and self.alias == other.alias
        return False

    def __str__(self):
        if self.joins is None:
            return ""
        else:
            result = "FROM "
            if self.database_name is not None:
                result += self.database_name + "."
            result += "`" + self.table_name + "`" if self.table_name[0] == "_" else self.table_name
            if self.alias is not None:
                result += " AS " + self.alias
            if len(self.joins) > 0:
                result += " "
                result += " ".join([str(join) for join in self.joins])
            return result

    def get_table_dependencies(self) -> Set[str]:
        return self._table_dependencies

    def accept(self, visitor):
        visitor.visit_from_clause(self)
