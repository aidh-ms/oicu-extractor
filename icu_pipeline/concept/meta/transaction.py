from icu_pipeline.concept.meta import AbstractMetaConcept
from icu_pipeline.mapper.schema.fhir.encounter import FHIREncounter
from icu_pipeline.mapper.schema.ohdsi import AbstractOHDSISinkSchema
from icu_pipeline.mapper.source import DataSource
from icu_pipeline.mapper.source.mimic.icustays import MimicICUEncounterMapper
from icu_pipeline.mapper.source.eicu.patient import EICUEncounterMapper


class ICUEncounter(AbstractMetaConcept):
    CONCEPT_ID = "icu_encounter"
    FHIR_SCHEMA = FHIREncounter
    OHDSI_SCHEMA = AbstractOHDSISinkSchema
    MAPPER = {
        DataSource.MIMIC: MimicICUEncounterMapper,
        DataSource.EICU: EICUEncounterMapper,
    }
