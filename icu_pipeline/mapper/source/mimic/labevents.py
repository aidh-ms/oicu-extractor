from abc import ABCMeta

from icu_pipeline.mapper.source.mimic import AbstractMimicEventsMapper


class AbstractMimicLabEventsMapper(AbstractMimicEventsMapper, metaclass=ABCMeta):
    SQL_QUERY = "SELECT * FROM mimiciv_hosp.labevents WHERE itemid = any(%(values)s);"


class MimicSerumCreatinineMapper(AbstractMimicLabEventsMapper):
    SQL_PARAMS = {"values": [50912, 52546]}


class MimicUreaMapper(AbstractMimicLabEventsMapper):
    SQL_PARAMS = {"values": [52647, 51006]}


class MimicHbMapper(AbstractMimicLabEventsMapper):
    SQL_PARAMS = {"values": [50811, 51222, 51640]}


class MimicArterialBloodLactateMapper(AbstractMimicLabEventsMapper):
    SQL_PARAMS = {"values": [50813, 52442]}


class MimicBloodSodiumMapper(AbstractMimicLabEventsMapper):
    SQL_PARAMS = {"values": [50824, 50983, 52455, 52623]}


class MimicPotassiumMapper(AbstractMimicLabEventsMapper):
    SQL_PARAMS = {"values": [50822, 50971, 52452, 52610]}


class MimicChlorideMapper(AbstractMimicLabEventsMapper):
    SQL_PARAMS = {"values": [50902, 50806, 52434, 52535]}


class MimicBilirubineMapper(AbstractMimicLabEventsMapper):
    SQL_PARAMS = {"values": [53089, 50885]}


class MimicGOTASTMapper(AbstractMimicLabEventsMapper):
    SQL_PARAMS = {"values": [50878]}


class MimicGPTAPTMapper(AbstractMimicLabEventsMapper):
    SQL_PARAMS = {"values": [50861]}


class MimicGGTMapper(AbstractMimicLabEventsMapper):
    SQL_PARAMS = {"values": [50927]}


class MimicSerumLDHMapper(AbstractMimicLabEventsMapper):
    SQL_PARAMS = {"values": [50954]}


class MimicINRMapper(AbstractMimicLabEventsMapper):
    SQL_PARAMS = {"values": [51675]}


class MimicLeukocyteCountMapper(AbstractMimicLabEventsMapper):
    SQL_PARAMS = {"values": [51301, 51755, 51756]}


class MimicPlateletCountMapper(AbstractMimicLabEventsMapper):
    SQL_PARAMS = {"values": [51265, 51704]}


class MimicCRPMapper(AbstractMimicLabEventsMapper):
    SQL_PARAMS = {"values": [50889]}


class MimicGlucoseMapper(AbstractMimicLabEventsMapper):
    SQL_PARAMS = {"values": [50931, 52027, 52569]}


class MimicMagnesiumMapper(AbstractMimicLabEventsMapper):
    SQL_PARAMS = {"values": [50960]}


class MimicCalciumMapper(AbstractMimicLabEventsMapper):
    SQL_PARAMS = {"values": [50893, 52034, 52035]}


class MimicPhosphateMapper(AbstractMimicLabEventsMapper):
    SQL_PARAMS = {"values": [50970]}
