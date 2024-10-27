from dataclasses import dataclass
from enum import StrEnum, auto
from typing import Any

from pydantic import BaseModel


class ConceptCoding(StrEnum):
    """
    Enum to define the coding system for the concept identifiers.
    """

    SNOMED = auto()
    LOINC = auto()


class DataSource(StrEnum):
    """
    Enum for the different data sources that can be queried.
    """

    MIMICIV = auto()
    AMDS = auto()
    EICU = auto()


@dataclass
class MapperConfig:
    """
    Dataclass to define the configuration for a mapper.
    """

    klass: str
    source: DataSource
    unit: str
    # TODO - Declare fixed set of parameters if possible
    params: dict[str, Any]


class ConceptConfig(BaseModel):
    """
    Dataclass to define the configuration for a concept.
    """

    name: str
    description: str
    identifiers: dict[ConceptCoding, str]
    unit: str
    # TODO - We either need the schema attribute, or the klass attribute but not both
    # schema: str
    mapper: list[MapperConfig]


@dataclass
class SourceConfig:
    """
    Configuration for the source mapper.s
    """

    connection: str
    chunksize: int = 10000
    # optional limit for the number of rows to be fetched
    limit: int = -1
