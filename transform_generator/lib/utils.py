import os
import shutil
import re
from transform_generator.lib.logging import get_logger

logger = get_logger(__name__)


def clear_dir(dir_path: str):
    logger.info("clear path dir_path: %s", dir_path)
    regexp = re.compile(r'^/$|^[a-z]:[/\\]$')
    if regexp.search(dir_path) is not None:
        raise Exception("Can not remove root directory " + dir_path)
    if os.path.exists(dir_path):
        shutil.rmtree(dir_path)


def write_file(output_file, query):
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    f = open(output_file, "w")
    f.write(query)
    f.close()


def replace_string_for_multi_env_supp(config_value, query):
    for multiple_env_support in config_value.table_names_to_support_multiple_env:
        if multiple_env_support:
            db_name_and_table_name = multiple_env_support.split(".")
            db_name = db_name_and_table_name[0].split("_")

            if config_value.target_language.lower() == 'impala':
                db_name[-2] += "${var:db_key}"
                multiple_env_support_new = "_".join(db_name) + "." + db_name_and_table_name[1]
                query = re.sub(re.escape(multiple_env_support), multiple_env_support_new, query, flags=re.IGNORECASE)
            elif config_value.target_language.lower() == 'hive':
                db_name[-2] += "${db_key}"
                multiple_env_support_new = "_".join(db_name) + "." +  db_name_and_table_name[1]
                query = re.sub(re.escape(multiple_env_support), multiple_env_support_new, query, flags=re.IGNORECASE)

    return query


def escape_characters(text: str) -> str:
    return text.replace("'", r"\'")


def to_be_deployed(filters, path):
    deploy = False
    for filter in filters:
        if filter in path:
            deploy = True
            break
    return deploy


