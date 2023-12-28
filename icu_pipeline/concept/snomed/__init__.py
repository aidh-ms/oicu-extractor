from abc import ABCMeta
from typing import Type

from icu_pipeline.concept import AbstractConcept
from icu_pipeline.mapper.source import AbstractSourceMapper, DataSource


class AbstractSnomedConcept(AbstractConcept, metaclass=ABCMeta):
    SNOMED_ID: str
    CONCEPT_TYPE = "snomed"

    def _map(
        self,
        source_mapper: Type[AbstractSourceMapper],
        source: DataSource,
    ):
        mapper = source_mapper(
            self.SNOMED_ID,
            self.CONCEPT_TYPE,
            self.FHIR_SCHEMA,
            self.OHDSI_SCHEMA,
            self._source_mapper_configs[source],
            self._sink_mapper,
            self._mapping_format,
        )

        mapper.map()
