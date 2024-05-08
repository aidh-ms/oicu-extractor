from typing import Type, Any, List, Dict, Generator
from dataclasses import dataclass
from importlib import import_module
import pandas as pd
from pydantic import BaseModel
from icu_pipeline.mapper.source import AbstractSourceMapper, DataSource
from icu_pipeline.mapper.schema.fhir import AbstractFHIRSinkSchema
from icu_pipeline.mapper.source import SourceMapperConfiguration


class ConceptConfig(BaseModel):
    @dataclass
    class MapperConfig:
        klass: str
        source: str
        params: Dict[str, Any]

    name: str
    description: str
    type: str
    id: str
    unit: str
    schema: Dict[str, str]
    mapper: Dict[str, MapperConfig]


class Concept:
    def __init__(
        self,
        concept_config: ConceptConfig,
        source_configs: dict[DataSource, SourceMapperConfiguration],
    ) -> None:
        # concept = concept_config["concept"]
        # self._concept_id: str = concept["id"]
        # self._concept_type: str = concept["type"]
        # schema_config = concept["schema"]
        # self._mapper: dict = concept["mapper"]  # Dictionary, DB_Name --> Config
        self._concept_config = concept_config
        self._source_configs = source_configs
        # TODO - Concepts should only use FHIR, transforms will be performed in later pipeline steps
        self._fhir_schema: Type[AbstractFHIRSinkSchema] = self._load_class(
            "icu_pipeline.mapper.schema.fhir", concept_config.schema["fhir"]
        )

    def _load_class(self, module_name: str, class_name: str) -> Type:
        module = import_module(module_name)
        return getattr(module, class_name)

    def map(self) -> Generator[pd.DataFrame, None, None]:
        assert all(
            [s in self._concept_config.mapper for s in self._source_configs.keys()]
        ), f"Not all Source have a mapper for Concept '{self._concept_config.id}'"
        # Yield a DataFrame-Chunk for each Source and for each Chunk
        for db in self._source_configs.keys():
            config = self._concept_config.mapper[db]
            source = config.source
            source_mapper = self._load_class(
                f"icu_pipeline.mapper.source.{source}", config.klass
            )

            for df_chunk in self._map(source_mapper, source, config.params):
                yield df_chunk

    def _map(
        self,
        source_mapper: Type[AbstractSourceMapper],
        source: DataSource,
        params: Dict[str, Any],
    ):
        mapper = source_mapper(
            self._concept_config.id,
            self._concept_config.type,
            self._fhir_schema,
            ohdsi_schema=None,
            source_mapper_config=self._source_configs[source],
            sink_mapper=None,
            mapping_format=None,
            **params,
        )
        return mapper.map()
