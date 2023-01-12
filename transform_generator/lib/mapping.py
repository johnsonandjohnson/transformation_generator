from multiprocessing import Pool
from os.path import split, join
from typing import List, Dict, Tuple

from transform_generator.parser.syntax_error_list_exception import SyntaxErrorListException
from transform_generator.generator.generator_error import GeneratorException, GeneratorError
from transform_generator.generator.transform_rule import TransformRule
from transform_generator.generator.transformation_generator_exception import TransformationGeneratorError
from transform_generator.lib.config_entry import ConfigEntry
from transform_generator.lib.data_mapping import DataMapping
from transform_generator.lib.logging import get_logger
from transform_generator.parser.ast.field import Field
from transform_generator.parser.ast.from_clause import FromClause
from transform_generator.parser.ast.select_query import SelectQuery
from transform_generator.parser.ast.transform_exp import TransformExp
from transform_generator.parser.ast.field import Field
from transform_generator.parser.syntax_error_list_exception import SyntaxErrorListException
from transform_generator.parser.transform_exp import get_ast_for_str
from transform_generator.reader.mapping_reader import read_mapping
from transform_generator.lib.data_mapping import DataMapping

from typing import List, Dict, Tuple
from os.path import split, join

from transform_generator.lib.logging import get_logger

from multiprocessing import Pool

logger = get_logger(__name__)
data_mapping_cache = {}


def process_transformation_rule(query, src_tables, transformation_rule):
    if transformation_rule.business_rule.strip() != '':
        ast = get_ast_for_str(transformation_rule.business_rule)
        if ast.from_clause is not None:
            query.merge_transform_exp(ast)
    if transformation_rule.src_db_name != '' and transformation_rule.src_table_name != '':
        src_table_name = transformation_rule.src_db_name + "." + transformation_rule.src_table_name
        if src_table_name not in src_tables:
            src_tables.add(src_table_name)


def get_from_clause(transformation_rules):
    """
    Loop through transformation rules business rules and return from_clause

    :param transformation_rules: - A sequence of TransformationRule objects
    :returns: from_clause
    """
    query = SelectQuery()
    src_tables = set()
    syntax_errors = []

    for i, transformation_rule in enumerate(transformation_rules):
        try:
            process_transformation_rule(query, src_tables, transformation_rule)
        except SyntaxErrorListException as ex:
            for err in ex.error_msg_list:
                syntax_errors.append(GeneratorError(i + 2, err))

    if len(syntax_errors) > 0:
        raise GeneratorException(syntax_errors)

    if query.from_clause is not None:
        return query.from_clause
    else:
        if len(src_tables) == 1:
            src_table = src_tables.pop().split(".")
            return FromClause(src_table[1], None, src_table[0])
        else:
            raise TransformationGeneratorError("Different Source Tables/Databases and no specified from/join clause")


def get_ast_for_datetime(from_clause, transformation_rule):
    if transformation_rule.tgt_data_type == transformation_rule.src_data_type or (
            transformation_rule.src_data_type.lower() == 'date'
            and transformation_rule.tgt_data_type.lower() == 'timestamp'):
        ast = TransformExp(Field(transformation_rule.src_column_name,
                                 transformation_rule.src_table_name), from_clause)
    else:
        ast = TransformExp(get_ast_for_str('CAST(' + transformation_rule.src_table_name + '.' +
                                           transformation_rule.src_column_name + ' AS ' +
                                           transformation_rule.tgt_data_type + ') '), from_clause)
    return ast


def get_asts_by_target_column(transformation_rules: List[TransformRule]):
    """
    Parse a sequence of transformation rules and return a dictionary of any corresponding Abstract Syntax Trees.

    The transformation rules in the sequence can be simple direct mappings, or contain a Transformation Expression.

    :param transformation_rules: - A sequence of TransformationRule objects
    :returns: A dictionary where the key is a string representing the Target Column name, and the value is an AST.

    """
    tgt_col_to_ast = {}
    syntax_errors = []

    from_clause = get_from_clause(transformation_rules)

    for i, transformation_rule in enumerate(transformation_rules):
        try:
            if transformation_rule.business_rule.strip() == '':
                ast = get_ast_for_datetime(from_clause, transformation_rule)
            else:
                ast_initial = get_ast_for_str(transformation_rule.business_rule)
                ast = TransformExp(ast_initial.result_column, from_clause, ast_initial.where_clause,
                                   ast_initial.group_by_clause, ast_initial.distinct)

            tgt_col_to_ast[transformation_rule.tgt_column_name] = ast
        except SyntaxErrorListException as ex:
            for err in ex.error_msg_list:
                syntax_errors.append(GeneratorError(i + 2, err))

    if len(syntax_errors) > 0:
        raise GeneratorException(syntax_errors)

    return tgt_col_to_ast


def validate_tgt_db_and_tbl(transformation_rule: TransformRule, tgt_db: str, tgt_tbl: str) -> bool:
    """
    Determine if the target database and target rule match the provided values.
    The target database and target rule must be consistent for the entire sheet.
    :param transformation_rule: The transformation rule to test.
    :param tgt_db: The expected target database.
    :param tgt_tbl: The expected target table.
    :return: A boolean with True indicating it is valid, and False indicating it is invalid.
    """
    return not transformation_rule.tgt_db_name != tgt_db or transformation_rule.tgt_table_name != tgt_tbl


def validate_mapping_sheet(transformation_rules):
    mapping_errors = []
    tgt_table = transformation_rules[0].tgt_table_name
    tgt_db = transformation_rules[0].tgt_db_name
    for i in range(1, len(transformation_rules)):
        if not validate_tgt_db_and_tbl(transformation_rules[i], tgt_db, tgt_table):
            mapping_errors.append(
                GeneratorError(i + 2, "Target database and table need to be same for all rules in a given sheet"))

    return mapping_errors


def load_mapping(file_path) -> Tuple[str, DataMapping]:
    filename = split(file_path)[1]
    filename = filename.replace("__", "/")
    file_path = join(*file_path.split("__"))

    try:
        logger.info("Loading data mapping file %s", split(filename)[1])
        transformation_rules = read_mapping(file_path)
        mapping_errors = validate_mapping_sheet(transformation_rules)
        if len(mapping_errors) > 0:
            raise GeneratorException(mapping_errors)

        asts_by_target_column = get_asts_by_target_column(transformation_rules)

        database_name = transformation_rules[0].tgt_db_name
        table_name = transformation_rules[0].tgt_table_name
        target_field_names = set([r.tgt_column_name.upper() for r in transformation_rules])

        comment_by_target_column_name = {}
        for r in transformation_rules:
            # it *should* be asserted that each target field name in a mapping file is unique -- TG should validate
            # that there are no duplicate TGT_COLUMN_NAMEs in a given mapping file, not just pass by any duplicates
            # like done below. Save for a separate issue
            if r.tgt_column_name.upper() not in comment_by_target_column_name:
                comment_by_target_column_name[r.tgt_column_name.upper()] = r.comment

        data_mapping = DataMapping(key=filename,
                                   database_name=database_name,
                                   table_name=table_name,
                                   target_column_names=target_field_names,
                                   ast_by_target_column_name=asts_by_target_column,
                                   comment_by_target_column_name=comment_by_target_column_name,
                                   config_entry=None,
                                   table_definition=None)
    except Exception as e:
        raise TransformationGeneratorError("File: {} \n{}".format(filename, str(e))) from e
    return filename, data_mapping


def load_mappings(mapping_path: str, mapping_filenames: List[str]) -> Dict[str, DataMapping]:
    """Loads all mapping files in a list, parses the business rules, and returns the nested dictionaries with ASTs

    Args:
        mapping_path: The base directory where all data mapping files can be found
        mapping_filenames: A list of data mapping filenames in the mapping_path to be loaded.
    returns:
        A dictionary with key of mapping filename and value of DataMapping object.
    """
    logger.info("Loading %d data mapping files", len(mapping_filenames))

    processed_mappings = {filename: data_mapping_cache[filename] for filename in mapping_filenames
                          if filename in data_mapping_cache}
    unprocessed_mappings = set(mapping_filenames) - processed_mappings.keys()

    # slashes in file is replaced with __ to uniquely identify the sub-folder path,
    # which serves as key to config_by_mapping_filename
    full_paths = [join(mapping_path, file.replace("/", "__").replace("\\", "__")) for file in unprocessed_mappings]

    # return mapping_items
    with Pool() as p:
        result = dict(p.map(load_mapping, full_paths))

    data_mapping_cache.update(result)
    processed_mappings.update(result)

    return processed_mappings
