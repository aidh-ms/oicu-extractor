from abc import ABC, abstractmethod
from enum import StrEnum, auto

from pandera.typing import DataFrame

from icu_pipeline.mapper.schema import AbstractSinkSchema


class MappingFormat(StrEnum):
    """
    Enum for mapping formats.
    """
    FHIR = auto()
    OHDSI = auto()


class AbstractSinkMapper(ABC):
    """
    Abstract sink mapper class.
    """
    @abstractmethod
    def to_output_format(
        self,
        df: DataFrame[AbstractSinkSchema],
        schema: AbstractSinkSchema,
        id: str,
    ) -> None:
        raise NotImplementedError
