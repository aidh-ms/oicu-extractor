from multiprocessing import Pool
from typing import Type

from icu_pipeline.concept import AbstractConcept
from icu_pipeline.mapper.sink import AbstractSinkMapper, MappingFormat
from icu_pipeline.mapper.source import SourceMapperConfiguration, DataSource


class Pipeline:
    """
    The main class used to represent the Pipeline for data processing.

    The Pipeline class is used to represent the pipeline for data processing. It is
    used for processing a list of concepts in parallel, using a pool of worker
    processes.

    Attributes
    ----------
    _concepts : list[Type[AbstractConcept]]
        a list of concepts to be processed
    _sink_mapper : AbstractSinkMapper
        a mapper that defines how the data is to be written out
    _mapping_format : MappingFormat
        the format of the mapping
    _source_mapper_configs : dict[DataSource, SourceMapperConfiguration]
        a dictionary mapping data sources to their configurations
    _processes : int, optional
        the number of worker processes. Defaults to 2.

    Methods
    -------
    _worker_func(concept: AbstractConcept)
        The function to be run by each worker process.
    transfrom()
        Executes the pipeline, processing each concept in parallel.
    """

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
        """
        The function to be run by each worker in the pool.
        """
        concept.map()

    def transfrom(self):
        """
        Executes the pipeline, processing each concept in parallel.
        """
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
