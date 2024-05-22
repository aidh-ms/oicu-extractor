import pytest
import yaml
from icu_pipeline.concept import Concept, ConceptConfig, DataSource, SourceMapperConfiguration, ConceptCoding


class TestConcept:
    @pytest.fixture
    def example_concept(self):
        with open("conceptbase/example.yml", "r") as concept_file:
            config = yaml.safe_load(concept_file)
            return Concept(ConceptConfig.model_validate(config), {DataSource.MIMIC: SourceMapperConfiguration()}, ConceptCoding.SNOMED)

    def test_conceptbase(self, example_concept):
        assert example_concept._concept_config.name == "HeartRate"
        assert example_concept._concept_config.schema == "FHIRObservation"
        assert example_concept._concept_config.identifiers == {
            "snomed": "364075005", "loinc": "8867-4"}

    # TODO: Implement with new event mapper
    # def test_map(self, example_concept):
    #     chunk = example_concept.map()
    #     assert chunk is not None
    #     assert next(chunk) is not None
