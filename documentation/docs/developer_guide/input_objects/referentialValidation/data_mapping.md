# Data Mapping

1. The key must exist in the `input_fields` field of a `ConfigEntry` object.
2. Only one `DataMapping` object may exist for a given `key`. Each `key` must be unique.
3. A `TableDefinition` object must exist whose `DATABASE` equals the `database_name` field and whose `TABLE NAME` equals the `target_table` field on the `DataMapping` object.
4. Each item within the `target_column_names` must exist in the `fields` set of the corresponding `TableDefinition` object.