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
    concept_paths = [*(Path(__file__).parent / "concepts").rglob("*.yaml")]

    pipeline = Pipeline(
        configs,
        CSVFileSinkMapper(),
        MappingFormat.FHIR,
    )

    pipeline.transform(["HeartRate.yml", "SystolicBloodPressure.yml"])
