import xlrd
import csv
from os.path import splitext, split

from transform_generator.generator.transform_rule import TransformRule


def extract_transformation_rules(mapping_object, mapping_object_type):
    if mapping_object_type == 'excel':
        colidx = dict((mapping_object.cell(0, i).value, i) for i in range(mapping_object.ncols))
        return [TransformRule(mapping_object.cell(row_idx, colidx['SRC_SYSTEM_NAME']).value.strip(),
                              mapping_object.cell(row_idx, colidx['SRC_TABLE_NAME']).value.strip(),
                              mapping_object.cell(row_idx, colidx['SRC_COLUMN_NAME']).value.strip(),
                              mapping_object.cell(row_idx, colidx['SRC_COLUMN_DATA_TYPE']).value.strip(),
                              mapping_object.cell(row_idx, colidx['TGT_SYSTEM_NAME']).value.strip(),
                              mapping_object.cell(row_idx, colidx['TGT_TABLE_NAME']).value.strip(),
                              mapping_object.cell(row_idx, colidx['TGT_COLUMN_NAME']).value.strip(),
                              mapping_object.cell(row_idx, colidx['TGT_COLUMN_DATA_TYPE']).value.strip(),
                              mapping_object.cell(row_idx, colidx['BUSINESS_RULE']).value,
                              mapping_object.cell(row_idx, colidx.get('COMMENTS', None)).value)
                for row_idx in range(1, mapping_object.nrows)]
    else:
        if mapping_object_type == 'csv':
            return [TransformRule(row['SRC_SYSTEM_NAME'].strip(),
                                  row['SRC_TABLE_NAME'].strip(),
                                  row['SRC_COLUMN_NAME'].strip(),
                                  row['SRC_COLUMN_DATA_TYPE'].strip(),
                                  row['TGT_SYSTEM_NAME'].strip(),
                                  row['TGT_TABLE_NAME'].strip(),
                                  row['TGT_COLUMN_NAME'].strip(),
                                  row['TGT_COLUMN_DATA_TYPE'].strip(),
                                  row['BUSINESS_RULE'],
                                  row.get('COMMENTS', None))
                    for row in mapping_object]


def read_excel_mapping(excel_file_name, sheet_number=0):
    workbook = xlrd.open_workbook(excel_file_name)
    xl_sheet = workbook.sheet_by_index(sheet_number)
    return extract_transformation_rules(xl_sheet, 'excel')


def read_csv_mapping(csv_file_name):
    data = []
    with open(csv_file_name, newline='\n') as csvfile:
        row_reader = csv.DictReader(csvfile, delimiter=',', quotechar='"')
        for row in row_reader:
            data.append(row)
    return extract_transformation_rules(data, 'csv')


def read_mapping(mapping_sheet_path: str):
    path, filename = split(mapping_sheet_path)
    ext = splitext(filename)[1]

    if ext == '.xlsx':
        return read_excel_mapping(mapping_sheet_path)

    elif ext == '.csv':
        return read_csv_mapping(mapping_sheet_path)
