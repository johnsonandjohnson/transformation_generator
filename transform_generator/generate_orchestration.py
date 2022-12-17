import argparse

from transform_generator.lib.datafactory.orchestration_pipeline import generate_orchestration_pipeline
from transform_generator.plugin.loader import get_plugin_loader

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process data mapping files.')
    parser.add_argument('--repository_dir', help='Path to local repos directory')
    parser.add_argument('--orchestration_output_dir', help='Path to desired directory for storing orchestration output')
    parser.add_argument('--project_config_path', help='Path to the generator config file')
    parser.add_argument('--database_param_prefix', default='parameterization',
                        help='String prefix for the ADF Pipelines where parameterization')
    parser.add_argument('--external_module_config_paths',
                        help='Semicolon (;) delimited string of paths to external module config directory paths')

    args = parser.parse_args()

    project_loader = get_plugin_loader().project_group_loader()
    project_group = project_loader.load_project_group(args.project_paths, args.project_base_dir)

    generate_orchestration_pipeline(project_group, args.orchestration_output_dir,
                                    args.project_config_path, args.database_param_prefix,
                                    args.external_module_config_paths)
