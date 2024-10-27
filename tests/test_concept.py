import os

import pytest
import yaml
from dotenv import load_dotenv

from icu_pipeline.concept import (
    Concept,
    ConceptCoding,
    ConceptConfig,
    DataSource,
    SourceConfig,
)


class TestConcept:
    @pytest.fixture
    def example_concept(self):
        load_dotenv()
        POSTGRES_USER = os.getenv("POSTGRES_USER")
        POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
        MIMIC_DB = os.getenv("MIMIC_DB")
        POSTGRES_HOST = os.getenv("POSTGRES_HOST")
        POSTGRES_PORT = os.getenv("POSTGRES_PORT")
        connection_string = (
            f"postgresql+psycopg://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{MIMIC_DB}"
        )

        with open("conceptbase/example.yml", "r") as concept_file:
            config = yaml.safe_load(concept_file)
            return Concept(
                ConceptConfig(**config),
                {DataSource.MIMICIV: SourceConfig(connection=connection_string)},
                ConceptCoding.SNOMED,
            )

    def test_conceptbase(self, example_concept: Concept):
        assert str(example_concept._concept_config.name) == "HeartRate"
        # assert example_concept._concept_config.schema == "FHIRObservation"
        assert example_concept._concept_config.identifiers == {
            "snomed": "364075005",
            "loinc": "8867-4",
        }

    # TODO: Implement with new event mapper
    # def test_map(self, example_concept):
    #     chunk = example_concept.map()
    #     assert chunk is not None
    #     assert next(chunk) is not None
