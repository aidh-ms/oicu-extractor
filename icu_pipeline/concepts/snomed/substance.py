from icu_pipeline.concepts.snomed import AbstractSnomedConcept
from icu_pipeline.mapper.schema.fhir.medication import FHIRMedicationStatement
from icu_pipeline.mapper.schema.ohdsi import AbstractOHDSISinkSchema
from icu_pipeline.mapper.source import DataSource
from icu_pipeline.mapper.source.mimic.inputevents import (
    MimicNorepinephrineMapper,
    MimicNorepinephrineMapper,
    MimicAdrenalineMapper,
    MimicVasopressineMapper,
    MimicDobutamineMapper,
    MimicVancomycineMapper,
    MimicDextrose5PercentMapper,
    MimicNaClMapper,
    MimicAlbumine25PercentMapper,
)


class Norepinephrine(AbstractSnomedConcept):
    CONCEPT_ID = "45555007"
    FHIR_SCHEMA = FHIRMedicationStatement
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: MimicNorepinephrineMapper}


class Adrenaline(AbstractSnomedConcept):
    CONCEPT_ID = "387362001"
    FHIR_SCHEMA = FHIRMedicationStatement
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: MimicAdrenalineMapper}


class Vasopressine(AbstractSnomedConcept):
    CONCEPT_ID = "77671006"
    FHIR_SCHEMA = FHIRMedicationStatement
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: MimicVasopressineMapper}


class Dobutamine(AbstractSnomedConcept):
    CONCEPT_ID = "387145002"
    FHIR_SCHEMA = FHIRMedicationStatement
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: MimicDobutamineMapper}


class Vancomycine(AbstractSnomedConcept):
    CONCEPT_ID = "372735009"
    FHIR_SCHEMA = FHIRMedicationStatement
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: MimicVancomycineMapper}


class Dextrose5Percent(AbstractSnomedConcept):
    CONCEPT_ID = "100347000"
    FHIR_SCHEMA = FHIRMedicationStatement
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: MimicDextrose5PercentMapper}


class NaCl(AbstractSnomedConcept):
    CONCEPT_ID = "101664001"
    FHIR_SCHEMA = FHIRMedicationStatement
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: MimicNaClMapper}


class Albumine25Percent(AbstractSnomedConcept):
    CONCEPT_ID = "347435009"
    FHIR_SCHEMA = FHIRMedicationStatement
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: MimicAlbumine25PercentMapper}
