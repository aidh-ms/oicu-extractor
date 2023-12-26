from icu_pipeline.concept import AbstractSnomedConcept
from icu_pipeline.mapper.schema.fhir.observation import FHIRObservation
from icu_pipeline.mapper.schema.fhir.medication import FHIRMedicationStatement
from icu_pipeline.mapper.schema.ohdsi import AbstractOHDSISinkSchema
from icu_pipeline.mapper.source import DataSource
from icu_pipeline.mapper.source.mimic.labevents import (
    MimicSerumCreatinineMapper,
    UreaMapper,
    HbMapper,
    ArterialBloodLactateMapper,
    BloodSodiumMapper,
    PotassiumMapper,
    ChlorideMapper,
    BilirubineMapper,
    GOTASTMapper,
    GPTAPTMapper,
    GGTMapper,
    SerumLDHMapper,
    INRMapper,
)
from icu_pipeline.mapper.source.mimic.chartevent import (
    MimicHeartRateMapper,
    SystolicBloodPressureInvasiveMapper,
    DiastolicBloodPressureInvasiveMapper,
    MeanArterialBloodPressureInvasiveMapper,
    SystolicBloodPressureNonInvasiveMapper,
    DiastolicBloodPressureNonInvasiveMapper,
    MeanArterialBloodPressureNonInvasiveMapper,
    OxygenSaturationMapper,
    TemperatureMapper,
    TemperatureFahrenheitMapper,
    WeightMapper,
    HeightMapper,
)
from icu_pipeline.mapper.source.mimic.derived import (
    UrineOutputMapper,
    ArterialPCO2Mapper,
    ArterialPO2Mapper,
    ArterialPHMapper,
    ArterialBicarbonateMapper,
    ArterialBaseexcessMapper,
    FiO2Mapper,
)
from icu_pipeline.mapper.source.mimic.patient import AgeMapper, GenderMapper
from icu_pipeline.mapper.source.mimic.inputevents import (
    NorepinephrineMapper,
    AdrenalineMapper,
    VasopressineMapper,
    DobutamineMapper,
)


class SerumCreatinine(AbstractSnomedConcept):
    SNOMED_ID = "113075003"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: MimicSerumCreatinineMapper}


class HeartRate(AbstractSnomedConcept):
    SNOMED_ID = "364075005"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: MimicHeartRateMapper}


class SystolicBloodPressureInvasive(AbstractSnomedConcept):
    SNOMED_ID = "251071003"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: SystolicBloodPressureInvasiveMapper}


class DiastolicBloodPressureInvasive(AbstractSnomedConcept):
    SNOMED_ID = "251073000"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: DiastolicBloodPressureInvasiveMapper}


class MeanArterialBloodPressureInvasive(AbstractSnomedConcept):
    SNOMED_ID = "251074006"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: MeanArterialBloodPressureInvasiveMapper}


class SystolicBloodPressureNonInvasive(AbstractSnomedConcept):
    SNOMED_ID = "251070002"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: SystolicBloodPressureNonInvasiveMapper}


class DiastolicBloodPressureNonInvasive(AbstractSnomedConcept):
    SNOMED_ID = "174255007"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: DiastolicBloodPressureNonInvasiveMapper}


class MeanArterialBloodPressureNonInvasive(AbstractSnomedConcept):
    SNOMED_ID = "251074006"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: MeanArterialBloodPressureNonInvasiveMapper}


class OxygenSaturation(AbstractSnomedConcept):
    SNOMED_ID = "431314004"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: OxygenSaturationMapper}


class Urea(AbstractSnomedConcept):
    SNOMED_ID = "250623007"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: UreaMapper}


class Hb(AbstractSnomedConcept):
    SNOMED_ID = "104141003"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: HbMapper}


class ArterialBloodLactate(AbstractSnomedConcept):
    SNOMED_ID = "372451000119107"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: ArterialBloodLactateMapper}


class BloodSodium(AbstractSnomedConcept):
    SNOMED_ID = "312469006"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: BloodSodiumMapper}


class Potassium(AbstractSnomedConcept):
    SNOMED_ID = "312468003"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: PotassiumMapper}


class Chloride(AbstractSnomedConcept):
    SNOMED_ID = "104589004"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: ChlorideMapper}


class Bilirubine(AbstractSnomedConcept):
    SNOMED_ID = "359986008"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: BilirubineMapper}


class GOTAST(AbstractSnomedConcept):
    SNOMED_ID = "45896001"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: GOTASTMapper}


class GPTAPT(AbstractSnomedConcept):
    SNOMED_ID = "250637003"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: GPTAPTMapper}


class GGT(AbstractSnomedConcept):
    SNOMED_ID = "60153001"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: GGTMapper}


class SerumLDH(AbstractSnomedConcept):
    SNOMED_ID = "273974004"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: SerumLDHMapper}


class INR(AbstractSnomedConcept):
    SNOMED_ID = "440685005"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: INRMapper}


class UrineOutput(AbstractSnomedConcept):
    SNOMED_ID = "404231008"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: UrineOutputMapper}


class Temperature(AbstractSnomedConcept):
    SNOMED_ID = "386725007"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: [TemperatureFahrenheitMapper, TemperatureMapper]}


class Height(AbstractSnomedConcept):
    SNOMED_ID = "1153637007"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: HeightMapper}


class Weight(AbstractSnomedConcept):
    SNOMED_ID = "726527001"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: WeightMapper}


class ArterialPO2(AbstractSnomedConcept):
    SNOMED_ID = "25579001"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: ArterialPO2Mapper}


class ArterialPCO2(AbstractSnomedConcept):
    SNOMED_ID = "167028004"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: ArterialPCO2Mapper}


class ArterialPH(AbstractSnomedConcept):
    SNOMED_ID = "27051004"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: ArterialPHMapper}


class ArterialBicarbonate(AbstractSnomedConcept):
    SNOMED_ID = "443685006"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: ArterialBicarbonateMapper}


class ArterialBaseexcess(AbstractSnomedConcept):
    SNOMED_ID = "67487000"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: ArterialBaseexcessMapper}


class FiO2(AbstractSnomedConcept):
    SNOMED_ID = "250774007"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: FiO2Mapper}


class Age(AbstractSnomedConcept):
    SNOMED_ID = "424144002"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: AgeMapper}


class Gender(AbstractSnomedConcept):
    SNOMED_ID = "263495000"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: GenderMapper}


class Norepinephrine(AbstractSnomedConcept):
    SNOMED_ID = "45555007"
    FHIR_SCHEMA = FHIRMedicationStatement
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: NorepinephrineMapper}


class Adrenaline(AbstractSnomedConcept):
    SNOMED_ID = "387362001"
    FHIR_SCHEMA = FHIRMedicationStatement
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: AdrenalineMapper}


class Vasopressine(AbstractSnomedConcept):
    SNOMED_ID = "77671006"
    FHIR_SCHEMA = FHIRMedicationStatement
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: VasopressineMapper}


class Dobutamine(AbstractSnomedConcept):
    SNOMED_ID = "387145002"
    FHIR_SCHEMA = FHIRMedicationStatement
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: DobutamineMapper}
