from abc import ABCMeta

from icu_pipeline.mapper.source.mimic import AbstractMimicEventsMapper


class AbstractMimicChartEventsMapper(AbstractMimicEventsMapper, metaclass=ABCMeta):
    SQL_QUERY = "SELECT * FROM mimiciv_icu.chartevents WHERE itemid = any(%(values)s);"


class MimicHeartRateMapper(AbstractMimicChartEventsMapper):
    SQL_PARAMS = {"values": [220045]}
