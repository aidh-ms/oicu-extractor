from icu_pipeline.concept.snomed import AbstractSnomedConcept
from icu_pipeline.mapper.schema.fhir.observation import FHIRObservation
from icu_pipeline.mapper.schema.ohdsi import AbstractOHDSISinkSchema
from icu_pipeline.mapper.source import DataSource
from icu_pipeline.mapper.source.mimic.chartevent import (
    WeightMapper,
)


class Weight(AbstractSnomedConcept):
    SNOMED_ID = "726527001"
    FHIR_SCHEMA = FHIRObservation
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {DataSource.MIMIC: WeightMapper}
