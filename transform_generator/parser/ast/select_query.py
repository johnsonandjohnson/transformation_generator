class SelectQuery:
    def __init__(self, result_columns=None, from_clause=None, where_clause=None, group_by_clause=None, distinct=False):
        self._distinct = distinct
        self._result_columns = [] if result_columns is None else result_columns
        self._from_clause = from_clause
        self._where_clause = where_clause
        self._group_by_clause = group_by_clause

    @property
    def distinct(self):
        return self._distinct

    @property
    def result_columns(self):
        return self._result_columns

    @property
    def from_clause(self):
        return self._from_clause

    @property
    def where_clause(self):
        return self._where_clause

    @property
    def group_by_clause(self):
        return self._group_by_clause

    def merge_transform_exp(self, transform_exp):
        """
        Merge all the elements of a TransformExp() object into this instance of SelectQuery.
        Result column, and From/Join statements will be combined where possible. For example, if two FROM clauses
        have the same ON condition in JOIN expressions, only one join will be added. However, if two joins to the same
        table have different ON conditions, than two separate joins will be created.
        Aliases of tables, and the corresponding result columns, may be dynamically changed to resolve collissions.

        @param transform_exp A TransformExp object to be merged into the this SelectQuery instance.
        """

        # Any merged expression being distinct will result in the query being distinct.
        if transform_exp.distinct:
            self._distinct = True

        self.result_columns.append(transform_exp.result_column)

        if transform_exp.group_by_clause is not None and len(transform_exp.group_by_clause.exp_list) > 0:
            if self.group_by_clause is None:
                self._group_by_clause = transform_exp.group_by_clause
            else:
                if self.group_by_clause != transform_exp.group_by_clause:
                    clause_1 = "Group By Clause 1: " + str(self.group_by_clause)
                    clause_2 = "Group By Clause 2: " + str(transform_exp.group_by_clause)
                    raise Exception("Cannot process different group by clauses." + "\n\t" + clause_1 + "\n\t" + clause_2)

                    # 3 cases
        # 1. This instance has a FromClause and Transform Exp does not -> do nothing
        # 2. Transform Exp has a from clause and this instance does not -> Assign from clause to this instance.
        # 3. Transform Exp and this instance both have from clauses ->  Perform a merge.
        if self.from_clause is None and transform_exp.from_clause is not None:
            self._from_clause = transform_exp.from_clause # Case 2
        else:
            if self.from_clause is not None and transform_exp.from_clause is not None: # Case 3
                if self.from_clause.table_name != transform_exp.from_clause.table_name:
                    raise Exception("Cannot merge FROM clauses with different main/driver tables.")
                else:
                    if self.from_clause.database_name is None and transform_exp.from_clause.database_name is not None:
                        self._from_clause = transform_exp.from_clause  # Case 2

                # Handle joins here
                for join in transform_exp.from_clause.joins:
                    match_found = False
                    for existing_join in self.from_clause.joins:
                        if join == existing_join:
                            match_found = True

                    if not match_found:
                        self.from_clause.joins.append(join)

        if transform_exp.where_clause is not None:
            self._where_clause = transform_exp.where_clause

    def accept(self, visitor):
        visitor.visit_select_query(self)
