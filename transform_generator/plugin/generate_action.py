from transform_generator.project import Project


class GenerateFilesAction:
    _registry = []

    @classmethod
    def register(cls, *args):
        """Register a `generate_files` function  as a plug-in."""
        def decorator(fn):
            fn._props = tuple(args)
            cls._registry.append(fn)
            return fn
        return decorator

    def generate_files(self, project_group: list[Project], target_path: str):
        """A generic function generating some collection of files based on the provided project group.

        Intended to be used to provide code to generate transformations, orchestration, lineage, etc. Register all
        applicable plugins and they will be executed at the appropriate times.

        Args:
            project_group: A list of projects containing the details for file generation
            target_path: The base path to output all files. Function may create sub-directories within.

        Returns:
            Nothing
        """

        pass

    @classmethod
    def generate_all(cls, project_group: list[Project], output_dir: str):
        for fn in cls._registry:
            fn(project_group, output_dir)

