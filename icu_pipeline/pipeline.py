from icu_pipeline.concept import AbstractSnomedConcept
from icu_pipeline.mapper.sink import AbstractSinkMapper, MappingFormat
from icu_pipeline.mapper.source import SourceMapperConfiguration, DataSource


class Pipeline:
    def __init__(
        self,
        concepts: list[AbstractSnomedConcept],
        sink_mapper: AbstractSinkMapper,
        mapping_format: MappingFormat,
        source_mapper_configs: dict[DataSource, SourceMapperConfiguration],
    ) -> None:
        self._concepts = concepts
        self._sink_mapper = sink_mapper
        self._mapping_format = mapping_format
        self._source_mapper_configs = source_mapper_configs

    def transfrom(self):
        for concept in self._concepts:
            _concept = concept(
                self._sink_mapper,
                self._mapping_format,
                self._source_mapper_configs,
            )

            _concept.map()
