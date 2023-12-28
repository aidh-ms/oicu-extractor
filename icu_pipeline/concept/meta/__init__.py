from abc import ABCMeta
from typing import Type

from icu_pipeline.concept import AbstractConcept
from icu_pipeline.mapper.source import AbstractSourceMapper


class AbstractMetaConcept(AbstractConcept, metaclass=ABCMeta):
    Meta_ID: str

    def _map(
        self,
        source_mapper: Type[AbstractSourceMapper],
        source: Type[AbstractSourceMapper],
    ):
        mapper = source_mapper(
            self.Meta_ID,
            self.FHIR_SCHEMA,
            self.OHDSI_SCHEMA,
            self._source_mapper_configs[source],
            self._sink_mapper,
            self._mapping_format,
        )

        mapper.map()
