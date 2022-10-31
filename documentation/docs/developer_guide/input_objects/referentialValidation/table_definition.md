# Table Definition

1. If a `DataMapping` has a `target_table`, then we must have a table definition for that table.
2. All fields in a `DataMapping` must be a subset of the fields defined in the `TableDefinition` of the specified `target_table`