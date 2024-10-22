from abc import ABC, abstractmethod
from enum import StrEnum, auto
from typing import Generator

from pandera.typing import DataFrame

from icu_pipeline.concept import Concept
from icu_pipeline.graph import Node
from icu_pipeline.schema import AbstractSinkSchema


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
        super().__init__(concept_id=None)  # TODO - Sink doesn't have a concept_id for now

    @abstractmethod
    def to_output_format(
        self,
        df_generator: Generator[DataFrame[AbstractSinkSchema], None, None],
        concept: Concept,
    ) -> None:
        raise NotImplementedError
