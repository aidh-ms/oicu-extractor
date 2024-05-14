from pathlib import Path

from icu_pipeline.pipeline import (
    Pipeline,
    DataSource,
    MappingFormat,
    SourceMapperConfiguration,
)
from icu_pipeline.mapper.sink.file import CSVFileSinkMapper


if __name__ == "__main__":
    configs = {
        DataSource.MIMIC: SourceMapperConfiguration()
    }

    pipeline = Pipeline(
        configs,
        CSVFileSinkMapper(),
        MappingFormat.FHIR,
    )

    pipeline.transform(["HeartRate", "SystolicBloodPressure"])
