from typing import Type, Any, List, Dict, Generator
from enum import StrEnum, auto
from dataclasses import dataclass
from importlib import import_module
import pandas as pd
from pydantic import BaseModel
from icu_pipeline.mapper.source import AbstractSourceMapper, DataSource
from icu_pipeline.mapper.schema.fhir import AbstractFHIRSinkSchema
from icu_pipeline.mapper.source import SourceMapperConfiguration


class ConceptCoding(StrEnum):
    SNOMED = auto()
    LOINC = auto()

@dataclass
class MapperConfig:
    klass: str
    source: str
    unit: str
    params: Dict[str, Any] # TODO - Declare fixed set of parameters if possible

class ConceptConfig(BaseModel):
    name: str
    description: str
    identifiers: Dict[ConceptCoding, str]
    unit: str
    schema: str
    mapper: List[MapperConfig]


class Concept:
    def __init__(
        self,
        concept_config: ConceptConfig,
        source_configs: dict[DataSource, SourceMapperConfiguration],
        concept_coding: ConceptCoding
    ) -> None:
        self._concept_config = concept_config
        self._source_configs = source_configs
        self._concept_coding = concept_coding
        # TODO - Concepts should only use FHIR, transforms will be performed in later pipeline steps
        self._fhir_schema: Type[AbstractFHIRSinkSchema] = self._load_class(
            "icu_pipeline.mapper.schema.fhir", concept_config.schema
        )

    def _load_class(self, module_name: str, class_name: str) -> Type:
        module = import_module(module_name)
        return getattr(module, class_name)

    def map(self) -> Generator[pd.DataFrame, None, None]:
        implemented_sources = [m.source for m in self._concept_config.mapper]
        assert all(
            [s in implemented_sources for s in self._source_configs.keys()]
        ), f"Not all Source have a mapper for Concept '{self._concept_config.name}'"
        # Yield a DataFrame-Chunk for each Source and for each Chunk
        for mapper in self._concept_config.mapper:
            source_mapper = self._load_class(
                f"icu_pipeline.mapper.source.{mapper.source}", mapper.klass
            )

            for df_chunk in self._map(source_mapper, mapper.source, mapper.params):
                yield df_chunk

    def _map(
        self,
        source_mapper: Type[AbstractSourceMapper],
        source: DataSource,
        params: Dict[str, Any],
    ):
        identifier = self._concept_config.identifiers[self._concept_coding]
        mapper = source_mapper(
            concept_id=identifier,
            concept_type=self._concept_coding,
            fhir_schema=self._fhir_schema,
            source_config=self._source_configs[source],
            **params,
        )
        return mapper.map()
