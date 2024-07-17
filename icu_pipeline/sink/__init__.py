from abc import ABC, abstractmethod
from enum import StrEnum, auto

from pandera.typing import DataFrame

from icu_pipeline.schema import AbstractSinkSchema
from icu_pipeline.graph import Node


class MappingFormat(StrEnum):
    """
    Enum for mapping formats.
    """
    FHIR = auto()
    OHDSI = auto()


class AbstractSinkMapper(ABC, Node):
    """
    Abstract sink mapper class.
    """
    def __init__(self) -> None:
        super().__init__(concept_id=None) # TODO - Sink doesn't have a concept_id for now

    @abstractmethod
    def to_output_format(
        self,
        df: DataFrame[AbstractSinkSchema],
        schema: AbstractSinkSchema,
        id: str,
    ) -> None:
        raise NotImplementedError
