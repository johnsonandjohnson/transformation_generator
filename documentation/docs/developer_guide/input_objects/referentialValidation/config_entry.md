# Config Entry

1. Multiple entires with the same `key` are permitted.
2. Multiple entries with the same `target_table` are permitted.
3. A `TableDefinition` object must exist whose `"DATABASE" + "." + "TABLE NAME"` equals the `target_table` field on the config_entry object.
4. Each item in the `input_files` field must have a corresponding `DataMapping` object with a matching key.