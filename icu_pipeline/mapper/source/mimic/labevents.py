from abc import ABCMeta

from icu_pipeline.mapper.source.mimic import AbstractMimicEventsMapper


class AbstractMimicLabEventsMapper(AbstractMimicEventsMapper, metaclass=ABCMeta):
    SQL_QUERY = "SELECT * FROM mimiciv_hosp.labevents WHERE itemid = any(%(values)s);"


class MimicSerumCreatinineMapper(AbstractMimicLabEventsMapper):
    SQL_PARAMS = {"values": [50912, 52546]}


class UreaMapper(AbstractMimicLabEventsMapper):
    SQL_PARAMS = {"values": [52647, 51006]}


class HbMapper(AbstractMimicLabEventsMapper):
    SQL_PARAMS = {"values": [50811, 51222, 51640]}


class ArterialBloodLactateMapper(AbstractMimicLabEventsMapper):
    SQL_PARAMS = {"values": [50813, 52442]}
