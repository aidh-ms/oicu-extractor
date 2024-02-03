from abc import ABCMeta

from icu_pipeline.mapper.source.eicu import AbstractEICUVitalMapper


class AbstractEICUVitalAPeriodicMapper(AbstractEICUVitalMapper, metaclass=ABCMeta):
    SQL_QUERY = """
        SELECT *, {field} as value FROM eicu_crd.vitalaperiodic 
        INNER JOIN eicu_crd.patient ON eicu_crd.vitalaperiodic.patientunitstayid = eicu_crd.patient.patientunitstayid
        ORDER BY eicu_crd.vitalaperiodic.patientunitstayid, observationoffset;
    """


class EICUSystolicBloodPressureNonInvasiveMapper(AbstractEICUVitalAPeriodicMapper):
    SQL_FIELDS = {"field": "noninvasivesystolic"}
    UNIT = ""  # TODO


class EICUDiastolicBloodPressureNonInvasiveMapper(AbstractEICUVitalAPeriodicMapper):
    SQL_FIELDS = {"field": "noninvasivediasystolic"}
    UNIT = ""  # TODO


class EICUMeanArterialBloodPressureNonInvasiveMapper(AbstractEICUVitalAPeriodicMapper):
    SQL_FIELDS = {"field": "noninvasivemean"}
    UNIT = ""  # TODO
