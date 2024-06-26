from typing import List, Dict
from pathlib import Path
import os.path as osp
from yaml import safe_load_all

from icu_pipeline.concept import Concept, ConceptConfig, ConceptCoding
from icu_pipeline.mapper.sink import AbstractSinkMapper, MappingFormat
from icu_pipeline.mapper.source import SourceMapperConfiguration, DataSource


class Pipeline:
    def __init__(
        self,
        source_configs: None | Dict[DataSource,
                                    SourceMapperConfiguration] = None,
        sink_mapper: None | AbstractSinkMapper = None,
        mapping_format=MappingFormat.FHIR,
        concept_coding=ConceptCoding.SNOMED,
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

    def _load_concepts(self, concepts: List[str | Path], base_path=None) -> List[Concept]:
        if base_path is None:
            base_path = osp.split(__file__)[:-2]
            base_path = osp.join(*base_path, "conceptbase", "concepts")
        out = []
        for next_concept in concepts:
            if isinstance(next_concept, str):
                with open(osp.join(base_path, f"{next_concept}.yml"), "r") as concept_file:
                    config = dict(*safe_load_all(concept_file))
                    out.append(
                        Concept(
                            ConceptConfig.model_validate(config),
                            self._source_configs,
                            concept_coding=self._concept_coding
                        )
                    )
            elif type(next_concept) in [Concept]:
                out.append(next_concept)
            else:
                raise TypeError(
                    f"Type of concept not recognized: '{type(next_concept)}'"
                )
        return out

    def _worker_func(self, concept: Concept):
        # TODO - Pure Proxy. Either remove or add some logic if appropriate
        #   Could become relevant if multiprocessing includes Queues
        return concept.map()

    def transform(self, concepts: List[Concept | str]):
        """Transform a list of Concepts according to given sources, steps, and sinks.
        Arguments:
          concepts: List of concepts. If (Concept) then use them directly.
            If (str) then expect a SNOMED ID."""
        assert concepts and len(
            concepts) > 0, "'concepts' is either None or empty."

        concepts: List[Concept] = self._load_concepts(concepts)

        for c in concepts:
            for db in self._source_configs.keys():
                avail_mappers = [k.source for k in c._concept_config.mapper]
                assert (
                    db in avail_mappers
                ), f"Database '{db}' is not in available sources {avail_mappers} for Concept '{c._concept_config.name}'"

        # TODO - ConceptName is easiert to handle/debug but might not be as unique as snomed?
        out = {c._concept_config.name: None for c in concepts}

        # Initialize Generators for all Concepts
        # TODO - SubProcesses need to use Queues in order
        #   to properly send chunks back to the main process.
        #   Skip multiprocessing for now and implement later.
        # with Pool(processes=self._processes) as worker_pool:
        #     out.append(worker_pool.map(self._worker_func, loaded_concepts))
        # return out

        # Concept-Generator gathers multiple sources sequentially.
        for c in concepts:
            out[c._concept_config.name] = self._worker_func(c)

        # TODO - Any intermediate steps would be added at this point
        #   For instance, transformation from FHIR to OHDSI
        #   or Filtering for specific Cohorts.

        # Run sink if not None
        if self._sink_mapper is not None:
            for c in concepts:
                result = self._sink_mapper.to_output_format(
                    df_generator=out[c._concept_config.name], concept=c
                )
                out[c._concept_config.name] = result
        return out
