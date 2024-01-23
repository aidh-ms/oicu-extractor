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
from dotenv import load_dotenv
import os

if __name__ == "__main__":
    load_dotenv()

    POSTGRES_USER = os.getenv("POSTGRES_USER")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    POSTGRES_HOST = os.getenv("POSTGRES_HOST")
    POSTGRES_PORT = os.getenv("POSTGRES_PORT")
    MIMIC_DB = os.getenv("MIMIC_DB")
    AMDS_DB = os.getenv("AMDS_DB")
    EICU_DB = os.getenv("EICU_DB")

    configs = {
        DataSource.MIMIC: SourceMapperConfiguration(
            f"postgresql+psycopg://{POSTGRES_USER}@{POSTGRES_HOST}/{MIMIC_DB}",
        ),
        DataSource.AMDS: SourceMapperConfiguration(
            f"postgresql+psycopg://{POSTGRES_USER}@{POSTGRES_HOST}/{AMDS_DB}",
        ),
    }

    pipeline = Pipeline(
        [HeartRate],
        CSVFileSinkMapper(),
        MappingFormat.FHIR,
        configs,
    )

    pipeline.transform()
