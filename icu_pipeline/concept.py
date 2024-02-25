from abc import ABC, abstractmethod
from typing import Type, Any
from importlib import import_module

from icu_pipeline.mapper.source import AbstractSourceMapper, DataSource
from icu_pipeline.mapper.schema.fhir import AbstractFHIRSinkSchema
from icu_pipeline.mapper.schema import AbstractSinkSchema
from icu_pipeline.mapper.schema.ohdsi import AbstractOHDSISinkSchema
from icu_pipeline.mapper.sink import AbstractSinkMapper, MappingFormat
from icu_pipeline.mapper.source import SourceMapperConfiguration, DataSource


class Concept:
    MAPPER: dict[
        DataSource, Type[AbstractSourceMapper] | list[Type[AbstractSourceMapper]]
    ]

    def __init__(
        self,
        concept_config: dict[str, Any],
        sink_mapper: AbstractSinkMapper,
        mapping_format: MappingFormat,
        source_mapper_configs: dict[DataSource, SourceMapperConfiguration],
    ) -> None:
        super().__init__()

        concept = concept_config["concept"]

        print(concept)

        self._concept_id: str = concept["id"]
        self._concept_type: str = concept["type"]

        schema_config = concept["schema"]
        self._fhir_schema: Type[AbstractFHIRSinkSchema] = self._load_class(
            "icu_pipeline.mapper.schema.fhir", schema_config["fhir"]
        )
        self._ohdsi_schema: Type[AbstractOHDSISinkSchema] = self._load_class(
            "icu_pipeline.mapper.schema.ohdsi", schema_config["ohdsi"]
        )

        self._mapper: list = concept["mapper"]
        self._sink_mapper = sink_mapper
        self._mapping_format = mapping_format
        self._source_mapper_configs = source_mapper_configs

    def _load_class(self, module_name: str, class_name: str) -> Type:
        module = import_module(module_name)
        return getattr(module, class_name)

    def map(self):
        for mapper_config in self._mapper:
            name, config = mapper_config.popitem()
            source = config["source"]
            source_mapper = self._load_class(
                f"icu_pipeline.mapper.source.{source}", config["class"]
            )

            self._map(source_mapper, source, config["params"])

    def _map(
        self,
        source_mapper: Type[AbstractSourceMapper],
        source: DataSource,
        params: dict[str, Any],
    ):
        print(params)

        mapper = source_mapper(
            self._concept_id,
            self._concept_type,
            self._fhir_schema,
            self._ohdsi_schema,
            self._source_mapper_configs[source],
            self._sink_mapper,
            self._mapping_format,
            **params,
        )

        mapper.map()
