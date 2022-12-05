import csv

from transform_generator.lib.table_definition import TableDefinition


def write_table_definition(table_definition_file_path: str, table_definition: TableDefinition):
    with open(table_definition_file_path, 'w', newline='') as csv_file:
        fieldnames = ["TABLE NAME", "TABLE BUSINESS NAME", "COLUMN NAME", "COLUMN DATA TYPE", \
                      "COLUMN NULLABLE", "COLUMN BUSINESS DESC", "PARTITION KEY", "PARTITION DATA TYPE",
                      "PARTITION COMMENT", "TABLE BUSINESS DESC"]

        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()

        for f in table_definition.fields.values():
            writer.writerow({"TABLE NAME": table_definition.table_name,
                             "COLUMN NAME": f.name,
                             "COLUMN DATA TYPE": f.data_type,
                             "COLUMN NULLABLE": f.nullable})



