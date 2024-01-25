from icu_pipeline.concept.snomed import AbstractSnomedConcept
from icu_pipeline.mapper.schema.fhir.observation import FHIRObservation
from icu_pipeline.mapper.schema.fhir.deviceusage import FHIRDeviceUsage
from icu_pipeline.mapper.schema.ohdsi import AbstractOHDSISinkSchema
from icu_pipeline.mapper.source import DataSource
from icu_pipeline.mapper.source.mimic.labevents import (
    MimicSerumCreatinineMapper,
    MimicUreaMapper,
    MimicHbMapper,
    MimicBloodSodiumMapper,
    MimicPotassiumMapper,
    MimicChlorideMapper,
    MimicBilirubineMapper,
    MimicGOTASTMapper,
    MimicGPTAPTMapper,
    MimicGGTMapper,
    MimicSerumLDHMapper,
    MimicINRMapper,
)
from icu_pipeline.mapper.source.mimic.derived import (
    MimicArterialPCO2Mapper,
    MimicArterialPO2Mapper,
    MimicArterialPHMapper,
    MimicArterialBicarbonateMapper,
    MimicArterialBaseexcessMapper,
)
from icu_pipeline.mapper.source.mimic.derived import (
    MimicDialysisMapper,
)


class SerumCreatinine(AbstractSnomedConcept):
    CONCEPT_ID = "113075003"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: MimicSerumCreatinineMapper}


class Urea(AbstractSnomedConcept):
    CONCEPT_ID = "250623007"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: MimicUreaMapper}


class Hb(AbstractSnomedConcept):
    CONCEPT_ID = "104141003"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: MimicHbMapper}


class ArterialPO2(AbstractSnomedConcept):
    CONCEPT_ID = "25579001"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: MimicArterialPO2Mapper}


class ArterialPCO2(AbstractSnomedConcept):
    CONCEPT_ID = "167028004"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: MimicArterialPCO2Mapper}


class ArterialPH(AbstractSnomedConcept):
    CONCEPT_ID = "27051004"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: MimicArterialPHMapper}


class ArterialBicarbonate(AbstractSnomedConcept):
    CONCEPT_ID = "443685006"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: MimicArterialBicarbonateMapper}


class ArterialBaseexcess(AbstractSnomedConcept):
    CONCEPT_ID = "67487000"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: MimicArterialBaseexcessMapper}


class BloodSodium(AbstractSnomedConcept):
    CONCEPT_ID = "312469006"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: MimicBloodSodiumMapper}


class Potassium(AbstractSnomedConcept):
    CONCEPT_ID = "312468003"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: MimicPotassiumMapper}


class Chloride(AbstractSnomedConcept):
    CONCEPT_ID = "104589004"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: MimicChlorideMapper}


class Bilirubine(AbstractSnomedConcept):
    CONCEPT_ID = "359986008"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: MimicBilirubineMapper}


class GOTAST(AbstractSnomedConcept):
    CONCEPT_ID = "45896001"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: MimicGOTASTMapper}


class GPTAPT(AbstractSnomedConcept):
    CONCEPT_ID = "250637003"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: MimicGPTAPTMapper}


class GGT(AbstractSnomedConcept):
    CONCEPT_ID = "60153001"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: MimicGGTMapper}


class SerumLDH(AbstractSnomedConcept):
    CONCEPT_ID = "273974004"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: MimicSerumLDHMapper}


class INR(AbstractSnomedConcept):
    CONCEPT_ID = "440685005"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: MimicINRMapper}


class Dialysis(AbstractSnomedConcept):
    CONCEPT_ID = "108241001"
    FHIR_SCHEMA = FHIRDeviceUsage
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: MimicDialysisMapper}
