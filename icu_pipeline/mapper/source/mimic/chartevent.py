from abc import ABCMeta

from icu_pipeline.mapper.source.mimic import AbstractMimicEventsMapper


class AbstractMimicChartEventsMapper(AbstractMimicEventsMapper, metaclass=ABCMeta):
    SQL_QUERY = "SELECT * FROM mimiciv_icu.chartevents WHERE itemid = any(%(values)s);"


class MimicHeartRateMapper(AbstractMimicChartEventsMapper):
    SQL_PARAMS = {"values": [220045]}


class SystolicBloodPressureInvasiveMapper(AbstractMimicChartEventsMapper):
    SQL_PARAMS = {"values": [220050]}


class DiastolicBloodPressureInvasiveMapper(AbstractMimicChartEventsMapper):
    SQL_PARAMS = {"values": [220051]}


class MeanArterialBloodPressureInvasiveMapper(AbstractMimicChartEventsMapper):
    SQL_PARAMS = {"values": [220052]}


class SystolicBloodPressureNonInvasiveMapper(AbstractMimicChartEventsMapper):
    SQL_PARAMS = {"values": [220179]}


class DiastolicBloodPressureNonInvasiveMapper(AbstractMimicChartEventsMapper):
    SQL_PARAMS = {"values": [220180]}
