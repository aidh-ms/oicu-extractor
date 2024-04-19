from multiprocessing import Pool
from typing import Type
from pathlib import Path

from yaml import safe_load_all

from icu_pipeline.concept import Concept
from icu_pipeline.mapper.sink import AbstractSinkMapper, MappingFormat
from icu_pipeline.mapper.source import SourceMapperConfiguration, DataSource


class Pipeline:
    def __init__(
        self,
        concept_paths: list[Path],
        sink_mapper: AbstractSinkMapper,
        mapping_format: MappingFormat,
        source_mapper_configs: dict[DataSource, SourceMapperConfiguration],
        processes: int = 2,
    ) -> None:
        self._concept_paths = concept_paths
        self._sink_mapper = sink_mapper
        self._mapping_format = mapping_format
        self._source_mapper_configs = source_mapper_configs
        self._processes = processes

    def _load_concepts(self, concept_paths: list[Path]) -> Concept:
        concepts = []
        for concept_path in concept_paths:
            with open(concept_path, "r") as concept_file:
                config = safe_load_all(concept_file)
                concepts.append(
                    Concept(
                        *config,
                        self._sink_mapper,
                        self._mapping_format,
                        self._source_mapper_configs,
                    )
                )
        return concepts

    def _worker_func(self, concept: Concept):
        concept.map()

    def transform(self):
        with Pool(processes=self._processes) as worker_pool:
            concepts = self._load_concepts(self._concept_paths)

            worker_pool.map(self._worker_func, concepts)
