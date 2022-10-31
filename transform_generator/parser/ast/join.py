class Join:
    def __init__(self, operator, table_name, condition, database_name=None, alias=None):
        self._operator = operator
        self._table_name = table_name
        self._condition = condition
        self._database_name = database_name
        self._alias = alias

    @property
    def operator(self):
        return self._operator

    @property
    def table_name(self):
        return self._table_name

    @property
    def condition(self):
        return self._condition

    @property
    def database_name(self):
        return self._database_name

    @property
    def alias(self):
        return self._alias

    def __str__(self):
        operator_prefix = self.operator + " " if self.operator is not None else ""
        on_suffix = " ON " + str(self.condition) if self.condition is not None else ""
        database_name = self.database_name + "." if self.database_name is not None else ""
        table_name = "`" + self.table_name + "`" if self.table_name[0] == "_" else self.table_name
        alias = " AS " + self.alias if self.alias is not None else ""
        return operator_prefix + "JOIN " + database_name + table_name + alias + on_suffix

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        db_match = self.database_name is None and other.database_name is None or \
            self.database_name == other.database_name

        return self.operator == other.operator and self.table_name == other.table_name and \
            self.condition == other.condition and self.alias == other.alias and db_match

    def accept(self, visitor):
        visitor.visit_join(self)
