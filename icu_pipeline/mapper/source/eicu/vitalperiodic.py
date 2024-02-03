from abc import ABCMeta

from icu_pipeline.mapper.source.eicu import AbstractEICUVitalMapper


class AbstractEICUVitalPeriodicMapper(AbstractEICUVitalMapper, metaclass=ABCMeta):
    SQL_QUERY = """
        SELECT *, {field} as value FROM eicu_crd.vitalperiodic 
        INNER JOIN eicu_crd.patient ON eicu_crd.vitalperiodic.patientunitstayid = eicu_crd.patient.patientunitstayid
        ORDER BY eicu_crd.vitalperiodic.patientunitstayid, observationoffset;
    """


class EICUHeartRateMapper(AbstractEICUVitalPeriodicMapper):
    SQL_FIELDS = {"field": "heartrate"}
    UNIT = ""  # TODO


class EICUSystolicBloodPressureInvasiveMapper(AbstractEICUVitalPeriodicMapper):
    SQL_FIELDS = {"field": "systemicsystolic"}
    UNIT = ""  # TODO


class EICUDiastolicBloodPressureInvasiveMapper(AbstractEICUVitalPeriodicMapper):
    SQL_FIELDS = {"field": "systemicdiastolic"}
    UNIT = ""  # TODO


class EICUMeanArterialBloodPressureInvasiveMapper(AbstractEICUVitalPeriodicMapper):
    SQL_FIELDS = {"field": "systemicmean"}
    UNIT = ""  # TODO


class EICUTemperatureMapper(AbstractEICUVitalPeriodicMapper):
    SQL_FIELDS = {"field": "temperature"}
    UNIT = ""  # TODO


class EICUOxygenSaturationMapper(AbstractEICUVitalPeriodicMapper):
    SQL_FIELDS = {"field": "saO2"}
    UNIT = ""  # TODO
