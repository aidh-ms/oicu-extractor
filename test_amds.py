from icu_pipeline.pipeline import (
    Pipeline,
    DataSource,
    MappingFormat,
    SourceMapperConfiguration,
)
from icu_pipeline.mapper.sink.file import CSVFileSinkMapper
from icu_pipeline.concept.snomed.observable_entity import (
    HeartRate,
)

if __name__ == "__main__":
    configs = {
        DataSource.MIMIC: SourceMapperConfiguration(
            "postgresql+psycopg://christian@localhost/mimiciv_demo"
        ),
        DataSource.AMDS: SourceMapperConfiguration(
            "postgresql+psycopg://christian@localhost/amds"
        ),
    }

    pipeline = Pipeline(
        [HeartRate],
        CSVFileSinkMapper(),
        MappingFormat.FHIR,
        configs,
    )

    pipeline.transform()
