from icu_pipeline.concept.snomed import AbstractSnomedConcept
from icu_pipeline.mapper.schema.fhir.observation import FHIRObservation
from icu_pipeline.mapper.schema.fhir.deviceusage import FHIRDeviceUsage
from icu_pipeline.mapper.schema.ohdsi import AbstractOHDSISinkSchema
from icu_pipeline.mapper.source import DataSource
from icu_pipeline.mapper.source.mimic.labevents import (
    MimicSerumCreatinineMapper,
    UreaMapper,
    HbMapper,
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
from icu_pipeline.mapper.source.mimic.derived import (
    ArterialPCO2Mapper,
    ArterialPO2Mapper,
    ArterialPHMapper,
    ArterialBicarbonateMapper,
    ArterialBaseexcessMapper,
)
from icu_pipeline.mapper.source.mimic.derived import (
    DialysisMapper,
)


class SerumCreatinine(AbstractSnomedConcept):
    SNOMED_ID = "113075003"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: MimicSerumCreatinineMapper}


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


class Dialysis(AbstractSnomedConcept):
    SNOMED_ID = "108241001"
    FHIR_SCHEMA = FHIRDeviceUsage
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: DialysisMapper}
