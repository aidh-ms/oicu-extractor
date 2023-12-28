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
)


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


class Vancomycine(AbstractSnomedConcept):
    SNOMED_ID = "372735009"
    FHIR_SCHEMA = FHIRMedicationStatement
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: VancomycineMapper}
