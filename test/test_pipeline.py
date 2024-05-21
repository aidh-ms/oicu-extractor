import pytest
import os
from icu_pipeline.pipeline import Pipeline, DataSource, SourceMapperConfiguration, MappingFormat
from icu_pipeline.mapper.sink.file import CSVFileSinkMapper


class TestPipeline:
    @pytest.fixture
    def setup_pipeline(self):
        source_configs = {DataSource.MIMIC: SourceMapperConfiguration()}
        sink_mapper = CSVFileSinkMapper()
        pipeline = Pipeline(source_configs, sink_mapper)
        example_concept = pipeline._load_concepts(["HeartRate"])
        return pipeline, example_concept

    def test_load_concepts(self, setup_pipeline):
        """
        Test for the _load_concepts method of the Pipeline class.
        Asserts that the method can successfully load the concepts "HeartRate" and "SystolicBloodPressure".
        """
        pipeline = setup_pipeline[0]
        assert pipeline._load_concepts(["HeartRate", "SystolicBloodPressure"])

    def test_worker_func(self, setup_pipeline):
        """
        Test for the _worker_func method of the Pipeline class.
        Asserts that the method is implemented and can process the example concept.
        """
        pipeline, example_concepts = setup_pipeline
        assert pipeline._worker_func(example_concepts[0])

    def test_run(self, setup_pipeline):
        """
        Test run for the whole pipeline implementation
        Asserts that the pipeline can be successfully executed.
        """
        configs = {
            DataSource.MIMIC: SourceMapperConfiguration()
        }

        pipeline = Pipeline(
            configs,
            CSVFileSinkMapper(),
            MappingFormat.FHIR,
        )

        pipeline.transform(["HeartRate"])
        # assert that the output is correctly created on root directory
        assert os.path.exists("output")
        assert os.path.exists("output/HeartRate.csv")
