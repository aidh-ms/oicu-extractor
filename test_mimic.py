from icu_pipeline.concepts.snomed.procedure import (
    GGT,
    GOTAST,
    GPTAPT,
    INR,
    ArterialBaseexcess,
    ArterialBicarbonate,
    ArterialPCO2,
    ArterialPH,
    ArterialPO2,
    Bilirubine,
    BloodSodium,
    Chloride,
    Dialysis,
    Hb,
    Potassium,
    SerumCreatinine,
    SerumLDH,
    Urea,
)
from icu_pipeline.concepts.snomed.qualifier_value import Weight
from icu_pipeline.concepts.snomed.substance import (
    Adrenaline,
    Albumine25Percent,
    Dextrose5Percent,
    Dobutamine,
    NaCl,
    Norepinephrine,
    Vancomycine,
    Vasopressine,
)
from icu_pipeline.pipeline import (
    Pipeline,
    DataSource,
    MappingFormat,
    SourceMapperConfiguration,
)
from icu_pipeline.mapper.sink.file import CSVFileSinkMapper
from icu_pipeline.concepts.snomed.observable_entity import (
    CRP,
    Age,
    ArterialBloodLactate,
    Calcium,
    DiastolicBloodPressureInvasive,
    DiastolicBloodPressureNonInvasive,
    FiO2,
    Gender,
    Glucose,
    HeartRate,
    Height,
    LeukocyteCount,
    Magnesium,
    MeanArterialBloodPressureInvasive,
    MeanArterialBloodPressureNonInvasive,
    OxygenSaturation,
    Phosphate,
    PlateletCount,
    SystolicBloodPressureInvasive,
    SystolicBloodPressureNonInvasive,
    Temperature,
    UrineOutput,
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
    MIMIC_DEMO_DB = os.getenv("MIMIC_DEMO_DB")
    AMDS_DB = os.getenv("AMDS_DB")
    EICU_DB = os.getenv("EICU_DB")

    configs = {
        DataSource.MIMIC: SourceMapperConfiguration(
            f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{
                POSTGRES_HOST}:{POSTGRES_PORT}/{MIMIC_DB}",
        ),
    }

    pipeline = Pipeline(
        ["conceptbase/concepts/HeartRate.yml"],
        CSVFileSinkMapper(),
        MappingFormat.FHIR,
        configs,
    )

    pipeline.transform()
