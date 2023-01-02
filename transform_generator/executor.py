from abc import ABC, abstractmethod
from typing import Any

from transform_generator.project import Project


class PipelineStage(ABC):
    def __init__(self, name: str):
        self.name = name

    @abstractmethod
    def execute(self, projects: list[Project], /, **kwargs) -> Any:
        pass


def execute_stages(projects: list[Project], stages: list[PipelineStage], /, **kwargs):
    results = dict(kwargs)
    for stage in stages:
        result = stage.execute(projects, **results)
        results[stage.name] = result
