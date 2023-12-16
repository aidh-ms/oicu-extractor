from icu_pipeline.concept import AbstractSnomedConcept
from icu_pipeline.mapper.schema.fhir import FHIRObservation
from icu_pipeline.mapper.schema.ohdsi import AbstractOHDSISinkSchema
from icu_pipeline.mapper.source import DataSource
from icu_pipeline.mapper.source.mimic.labevents import MimicSerumCreatinineMapper


class SerumCreatinine(AbstractSnomedConcept):
    SNOMED_ID = "113075003"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: MimicSerumCreatinineMapper}
