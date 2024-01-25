from multiprocessing import Pool
from typing import Type

from icu_pipeline.concept import AbstractConcept
from icu_pipeline.mapper.sink import AbstractSinkMapper, MappingFormat
from icu_pipeline.mapper.source import SourceMapperConfiguration, DataSource


class Pipeline:
    def __init__(
        self,
        concepts: list[Type[AbstractConcept]],
        sink_mapper: AbstractSinkMapper,
        mapping_format: MappingFormat,
        source_mapper_configs: dict[DataSource, SourceMapperConfiguration],
        processes: int = 2,
    ) -> None:
        self._concepts = concepts
        self._sink_mapper = sink_mapper
        self._mapping_format = mapping_format
        self._source_mapper_configs = source_mapper_configs
        self._processes = processes

    def _worker_func(self, concept: AbstractConcept):
        concept.map()

    def transform(self):
        with Pool(processes=self._processes) as worker_pool:
            concepts = [
                concept(
                    self._sink_mapper,
                    self._mapping_format,
                    self._source_mapper_configs,
                )
                for concept in self._concepts
            ]

            worker_pool.map(self._worker_func, concepts)
