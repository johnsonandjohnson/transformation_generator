from .from_clause import FromClause
from .group_by_clause import GroupByClause


class TransformExp:
    def __init__(self, result_column, from_clause: FromClause = None, where_clause=None,
                 group_by_clause: GroupByClause = None, distinct=False):
        self._result_column = result_column
        self._from_clause = from_clause
        self._where_clause = where_clause
        self._group_by_clause = group_by_clause
        self._distinct = distinct

    @property
    def result_column(self):
        return self._result_column

    @property
    def from_clause(self) -> FromClause:
        return self._from_clause

    @property
    def where_clause(self):
        return self._where_clause

    @property
    def group_by_clause(self) -> GroupByClause:
        return self._group_by_clause
    
    @property
    def distinct(self):
        return self._distinct

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            if self._distinct != other._distinct:
                return False
            
            from_clause_match = self.from_clause is None and other.from_clause is None or \
                self.from_clause == other.from_clause
            where_clause_match = self.where_clause is None and other.where_clause is None or \
                self.where_clause == other.where_clause
            group_by_clause_match = self.group_by_clause is None and other.group_by_clause is None or \
                self.group_by_clause == other.group_by_clause

            return self.result_column == other.result_column and from_clause_match and where_clause_match \
                and group_by_clause_match
        return False

    def __str__(self):        
        result = "DISTINCT " if self._distinct else ""
        result += str(self.result_column)
        
        if self.from_clause is not None:
            result += " " + (str(self.from_clause) if self.from_clause is not None else "")
        if self.where_clause is not None:
            result += " WHERE " + (str(self.where_clause) if self.where_clause is not None else "")
        if self.group_by_clause is not None:
            result += " " + (str(self.group_by_clause) if self.group_by_clause is not None else "")

        return result

    def __repr__(self):
        return str(self)

    def accept(self, visitor):
        visitor.visit_transform_exp(self)
