from pathlib import Path
from typing import Generator

from pandera.typing import DataFrame
from yaml import safe_load_all

from icu_pipeline.concept import Concept, ConceptCoding, ConceptConfig
from icu_pipeline.graph import Node
from icu_pipeline.graph.base import Graph
from icu_pipeline.job import Job
from icu_pipeline.logger import ICULogger
from icu_pipeline.sink import AbstractSinkMapper, MappingFormat
from icu_pipeline.source import DataSource, SourceConfig, getDataSampler

logger = ICULogger.get_logger()


class Pipeline:
    def __init__(
        self,
        source_configs: dict[DataSource, SourceConfig],
        sink_mapper: AbstractSinkMapper,
        mapping_format: MappingFormat = MappingFormat.FHIR,
        concept_coding: ConceptCoding = ConceptCoding.SNOMED,
        processes: int = 2,
    ) -> None:
        """A Pipeline that extracts, transforms, and loads data into sinks.
        Arguments:
          sources: (List[str]) List of ICU Databases that are supposed to be queries.
            Available DBs: mimic, amds, eicu, sicdb
        """
        assert len(source_configs) > 0, "No sources were passed."
        self._sink_mapper = sink_mapper
        self._mapping_format = mapping_format
        self._source_configs = source_configs
        self._concept_coding = concept_coding
        self._processes = processes
        self._graph = Graph()

    def _load_concepts(
        self, concepts: list[str | Path | Concept], base_path: str | Path | None = None
    ) -> list[Concept]:
        if base_path is None:
            base_path = Path(__file__).parent.parent / "conceptbase" / "concepts"
        if isinstance(base_path, str):
            base_path = Path(base_path)
        out: list = []
        # Check and add all concepts
        for next_concept in concepts:
            # Strings are ConceptNames -> filename
            if isinstance(next_concept, (str, Path)):
                concept_path = next_concept if isinstance(next_concept, Path) else base_path / f"{next_concept}.yml"
                with open(concept_path, "r") as concept_file:
                    config = dict(*safe_load_all(concept_file))
                    out.append(
                        Concept(
                            concept_config=ConceptConfig(**config),
                            source_configs=self._source_configs,
                            concept_coding=self._concept_coding,
                        )
                    )
            elif isinstance(next_concept, Concept):
                out.append(next_concept)
            else:
                raise TypeError(f"Type of concept not recognized: '{type(next_concept)}'")
        return out

    def transform(
        self, concepts: list[str | Concept], base_path: str | None = None
    ) -> Generator[DataFrame, None, None]:
        """Transform a list of Concepts according to given sources, steps, and sinks.
        Arguments:
          concepts: List of concepts. If (Concept) then use them directly.
            If (str) then expect a SNOMED ID."""
        assert concepts and len(concepts) > 0, "'concepts' is either None or empty."

        self._graph = Graph()

        concepts: list[Concept] = self._load_concepts(concepts, base_path)
        concept_id_to_node: dict[str, Concept] = {}

        #########################
        # Create left-to-right
        #########################
        for c in concepts:
            assert isinstance(c, Concept)
            for db in self._source_configs.keys():
                avail_mappers = [k.source for k in c._concept_config.mapper]
                assert (
                    db in avail_mappers
                ), f"Database '{db}' is not in available sources {avail_mappers} for Concept '{c._concept_config.name}'"
            concept_id_to_node[c._concept_id] = c

        # Attach default converters
        for v in concept_id_to_node:
            next_concept = concept_id_to_node[v]
            next_converter = next_concept.getDefaultConverter()
            self._graph.addPipe(next_concept, next_converter)
            concept_id_to_node[v] = next_converter

        # TODO - Attach Filter
        #   Pass

        # Attach Sinks
        for _, v in concept_id_to_node.items():
            self._graph.addPipe(source=v, sink=self._sink_mapper)

        ##############################
        # Backpropagate Dependencies
        ##############################
        def _attachDependencies(n: Node):
            out = 0
            for d in n.REQUIRED_CONCEPTS:
                if d not in concept_id_to_node:
                    # Create Concept
                    next_concept = self._load_concepts([d], base_path)[0]
                    next_converter = next_concept.getDefaultConverter()
                    # Attach Concept to Converter
                    self._graph.addPipe(next_concept, next_converter)
                    # Attach Converter to Original Node
                    self._graph.addPipe(next_converter, n)
                    # Add Mapping
                    concept_id_to_node[next_concept._concept_id] = next_converter
                    out += 1
                    # Check if converter has dependencies
                    out += _attachDependencies(next_converter)

            # Repeat for all sources
            for s in n._sources.values():
                out += _attachDependencies(s._source)
            return out

        added_dependencies = _attachDependencies(self._sink_mapper)
        logger.debug(f"Added {added_dependencies} new dependencies to the Graph.")

        # TODO - Implement Checks
        # graph.check_circularity()
        # graph.check_hanging_leaves()
        # graph.check_missing_connections()

        print(self._graph)

        for data_source, source_config in self._source_configs.items():
            next_sampler = getDataSampler(data_source, source_config)
            for next_chunk in next_sampler.get_samples():
                yield self._sink_mapper.get_data(job=Job(jobID="test", database=data_source, subjects=next_chunk))
