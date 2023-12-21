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


class BloodSodiumMapper(AbstractMimicLabEventsMapper):
    SQL_PARAMS = {"values": [50824, 50983, 52455, 52623]}


class PotassiumMapper(AbstractMimicLabEventsMapper):
    SQL_PARAMS = {"values": [50822, 50971, 52452, 52610]}


class ChlorideMapper(AbstractMimicLabEventsMapper):
    SQL_PARAMS = {"values": [50902, 50806, 52434, 52535]}


class BilirubineMapper(AbstractMimicLabEventsMapper):
    SQL_PARAMS = {"values": [53089, 50885]}


class GOTASTMapper(AbstractMimicLabEventsMapper):
    SQL_PARAMS = {"values": [50878]}


class GPTAPTMapper(AbstractMimicLabEventsMapper):
    SQL_PARAMS = {"values": [50861]}


class GGTMapper(AbstractMimicLabEventsMapper):
    SQL_PARAMS = {"values": [50927]}
