from icu_pipeline.concept.snomed.procedure import (
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
from icu_pipeline.concept.snomed.qualifier_value import Weight
from icu_pipeline.concept.snomed.substance import (
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
from icu_pipeline.concept.snomed.observable_entity import (
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
        [
            HeartRate,
            SystolicBloodPressureInvasive,
            DiastolicBloodPressureInvasive,
            MeanArterialBloodPressureInvasive,
            SystolicBloodPressureNonInvasive,
            DiastolicBloodPressureNonInvasive,
            MeanArterialBloodPressureNonInvasive,
            UrineOutput,
            Temperature,
            FiO2,
            OxygenSaturation,
            ArterialBloodLactate,
            Height,
            Age,
            Gender,
            LeukocyteCount,
            PlateletCount,
            CRP,
            Glucose,
            Magnesium,
            Calcium,
            Phosphate,
            SerumCreatinine,
            Urea,
            Hb,
            ArterialPO2,
            ArterialPCO2,
            ArterialPH,
            ArterialBicarbonate,
            ArterialBaseexcess,
            BloodSodium,
            Potassium,
            Chloride,
            Bilirubine,
            GOTAST,
            GPTAPT,
            GGT,
            SerumLDH,
            INR,
            Dialysis,
            Weight,
            Norepinephrine,
            Adrenaline,
            Vasopressine,
            Dobutamine,
            Vancomycine,
            Dextrose5Percent,
            NaCl,
            Albumine25Percent,
        ],
        CSVFileSinkMapper(),
        MappingFormat.FHIR,
        configs,
    )

    pipeline.transform()
