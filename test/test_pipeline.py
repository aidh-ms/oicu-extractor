import pytest
from icu_pipeline.pipeline import Pipeline, DataSource, SourceMapperConfiguration
from icu_pipeline.mapper.sink.file import CSVFileSinkMapper


@pytest.fixture
def setup_pipeline():
    source_configs = {DataSource.MIMIC: SourceMapperConfiguration()}
    sink_mapper = CSVFileSinkMapper()
    pipeline = Pipeline(source_configs, sink_mapper)
    example_concept = pipeline._load_concepts(["HeartRate"])
    return pipeline, example_concept


def test_load_concepts(setup_pipeline):
    pipeline = setup_pipeline[0]
    assert pipeline._load_concepts(["HeartRate", "SystolicBloodPressure"])


def test_worker_func(setup_pipeline):
    # assert the function is implemented
    pipeline, example_concepts = setup_pipeline
    assert pipeline._worker_func(example_concepts[0])
