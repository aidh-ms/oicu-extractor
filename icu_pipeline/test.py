from icu_pipeline.pipeline import (
    Pipeline,
    DataSource,
    MappingFormat,
    SourceMapperConfiguration,
)
from icu_pipeline.mapper.sink.file import CSVFileSinkMapper
from icu_pipeline.concept.snomed import SerumCreatinine

configs = {
    DataSource.MIMIC: SourceMapperConfiguration(
        "postgresql+psycopg://paul@localhost/mimiciv"
    ),
}

pipeline = Pipeline(
    [SerumCreatinine],
    CSVFileSinkMapper(),
    MappingFormat.FHIR,
    configs,
)

pipeline.transfrom()
