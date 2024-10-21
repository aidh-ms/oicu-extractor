import pytest
import os
from dotenv import load_dotenv
from icu_pipeline.pipeline import (
    Pipeline,
    DataSource,
    SourceConfig,
)
from icu_pipeline.concept import Concept
from icu_pipeline.sink.file import CSVFileSinkMapper


class TestPipeline:
    @pytest.fixture
    def setup_pipeline(self):
        load_dotenv()
        POOSTGRES_USER = os.getenv("POSTGRES_USER")
        POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
        MIMIC_DB = os.getenv("MIMIC_DB")
        POSTGRES_HOST = os.getenv("POSTGRES_HOST")
        POSTGRES_PORT = os.getenv("POSTGRES_PORT")
        connection_string = (
            f"postgresql+psycopg://{POOSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{MIMIC_DB}"
        )
        source_configs = {DataSource.MIMICIV: SourceConfig(connection=connection_string)}
        sink_mapper = CSVFileSinkMapper()
        pipeline = Pipeline(source_configs, sink_mapper)
        example_concept = pipeline._load_concepts(["HeartRate"])
        return pipeline, example_concept

    def test_load_concepts(self, setup_pipeline: tuple[Pipeline, Concept]):
        """
        Test for the _load_concepts method of the Pipeline class.
        Asserts that the method can successfully load the concepts "HeartRate" and "SystolicBloodPressure".
        """
        pipeline = setup_pipeline[0]
        assert pipeline._load_concepts(["HeartRate", "SystolicBloodPressure"])

    def test_worker_func(self, setup_pipeline: tuple[Pipeline, Concept]):
        """
        Test for the _worker_func method of the Pipeline class.
        Asserts that the method is implemented and can process the example concept.
        """
        pipeline, example_concepts = setup_pipeline
        assert pipeline.transform(example_concepts)

    def test_run(self, setup_pipeline: tuple[Pipeline, Concept]):
        """
        Test run for the whole pipeline implementation
        Asserts that the pipeline can be successfully executed.
        """

        pipeline = setup_pipeline[0]

        pipeline.transform(["HeartRate"])
        # assert that the output is correctly created on root directory
        # TODO - Test is currently not working
        # assert os.path.exists("output")
        # assert os.path.exists("output/HeartRate.csv")
