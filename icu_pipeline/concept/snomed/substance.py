from icu_pipeline.concept.snomed import AbstractSnomedConcept
from icu_pipeline.mapper.schema.fhir.medication import FHIRMedicationStatement
from icu_pipeline.mapper.schema.ohdsi import AbstractOHDSISinkSchema
from icu_pipeline.mapper.source import DataSource
from icu_pipeline.mapper.source.mimic.inputevents import (
    NorepinephrineMapper,
    NorepinephrineMapper,
    AdrenalineMapper,
    VasopressineMapper,
    DobutamineMapper,
    VancomycineMapper,
    Dextrose5PercentMapper,
    NaClMapper,
    Albumine20PercentMapper
)


class Norepinephrine(AbstractSnomedConcept):
    CONCEPT_ID = "45555007"
    FHIR_SCHEMA = FHIRMedicationStatement
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: NorepinephrineMapper}


class Adrenaline(AbstractSnomedConcept):
    CONCEPT_ID = "387362001"
    FHIR_SCHEMA = FHIRMedicationStatement
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: AdrenalineMapper}


class Vasopressine(AbstractSnomedConcept):
    CONCEPT_ID = "77671006"
    FHIR_SCHEMA = FHIRMedicationStatement
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: VasopressineMapper}


class Dobutamine(AbstractSnomedConcept):
    CONCEPT_ID = "387145002"
    FHIR_SCHEMA = FHIRMedicationStatement
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: DobutamineMapper}


class Vancomycine(AbstractSnomedConcept):
    CONCEPT_ID = "372735009"
    FHIR_SCHEMA = FHIRMedicationStatement
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: VancomycineMapper}



class Dextrose5Persent(AbstractSnomedConcept):
    CONCEPT_ID = "100347000"
    FHIR_SCHEMA = FHIRMedicationStatement
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: Dextrose5PercentMapper}


class NaCl(AbstractSnomedConcept):
    CONCEPT_ID = "101664001"
    FHIR_SCHEMA = FHIRMedicationStatement
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: NaClMapper}


class Albumine20Percent(AbstractSnomedConcept):
    CONCEPT_ID = "347435009"
    FHIR_SCHEMA = FHIRMedicationStatement
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: Albumine20PercentMapper}
