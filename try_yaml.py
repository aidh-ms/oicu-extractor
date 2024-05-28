
import os
from dotenv import load_dotenv
from icu_pipeline.pipeline import (
    Pipeline,
    DataSource,
    MappingFormat,
    SourceMapperConfiguration,
)
from icu_pipeline.mapper.sink.file import CSVFileSinkMapper


if __name__ == "__main__":
    load_dotenv()
    POSTGRES_USER = os.getenv("POSTGRES_USER")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    MIMIC_DB = os.getenv("MIMIC_DB")
    POSTGRES_HOST = os.getenv("POSTGRES_HOST")
    POSTGRES_PORT = os.getenv("POSTGRES_PORT")
    configs = {
        DataSource.MIMIC: SourceMapperConfiguration(
            # connection string
            f"postgresql+psycopg://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{
                POSTGRES_HOST}:{POSTGRES_PORT}/{MIMIC_DB}",
        )
    }

    pipeline = Pipeline(
        configs,
        CSVFileSinkMapper(),
        MappingFormat.FHIR,
    )

    pipeline.transform(["HeartRate"])
