import re


class TransformRule:
    def __init__(self, src_db_name, src_table_name, src_column_name, src_data_type, tgt_db_name, tgt_table_name, tgt_column_name, tgt_data_type, business_rule, comment=None):
        self._src_db_name = src_db_name
        self._src_table_name = src_table_name
        self._src_column_name = src_column_name
        self._src_data_type = src_data_type
        self._tgt_db_name = tgt_db_name
        self._tgt_table_name = tgt_table_name
        self._tgt_column_name = tgt_column_name
        self._tgt_data_type = tgt_data_type
        if tgt_data_type.lower() in ["int", "smallint", "bigint"]:
            self._business_rule = str(business_rule).split(".")[0] \
                if re.match(r'^\s*[0-9]+\.[0-9]+\s*$', str(business_rule)) else str(business_rule)
        else:
            self._business_rule = business_rule
        self._comment = comment

    @property
    def src_db_name(self):
        return self._src_db_name

    @property
    def src_table_name(self):
        return self._src_table_name

    @property
    def src_column_name(self):
        return self._src_column_name

    @property
    def src_data_type(self):
        return self._src_data_type

    @property
    def tgt_db_name(self):
        return self._tgt_db_name

    @property
    def tgt_table_name(self):
        return self._tgt_table_name

    @property
    def tgt_column_name(self):
        return self._tgt_column_name

    @property
    def tgt_data_type(self):
        return self._tgt_data_type

    @property
    def business_rule(self):
        return self._business_rule

    @property
    def comment(self):
        return self._comment

    def __str__(self):
        return "src_db_name %s src_table_name %s src_column_name %s src_data_type %s\n" \
               "tgt_db_name %s tgt_table_name %s tgt_column_name %s tgt_data_type %s business_rule %s" \
           % (self._src_db_name, self._src_table_name, self._src_column_name, self.src_data_type,
              self._tgt_db_name, self._tgt_table_name, self._tgt_column_name, self.tgt_data_type, self._business_rule)
