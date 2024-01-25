from abc import ABCMeta
from icu_pipeline.mapper.source.amds import AbstractAmdsEventsMapper


class AbstractAmdsNumericItemsMapper(AbstractAmdsEventsMapper, metaclass=ABCMeta):
    SQL_QUERY = (
        "SELECT * FROM amsterdamumcdb.numericitems WHERE itemid = any(%(values)s);"
    )


class AmdsHeartRateMapper(AbstractAmdsNumericItemsMapper):
    SQL_PARAMS = {"values": [6640]}


class AmdsSystolicBloodPressureInvasiveMapper(AbstractAmdsNumericItemsMapper):
    SQL_PARAMS = {"values": [6641]}


class AmdsDiastolicBloodPressureInvasiveMapper(AbstractAmdsNumericItemsMapper):
    SQL_PARAMS = {"values": [6643]}


class AmdsMeanArterialBloodPressureInvasiveMapper(AbstractAmdsNumericItemsMapper):
    SQL_PARAMS = {"values": [6642]}


class AmdsSystolicBloodPressureNonInvasiveMapper(AbstractAmdsNumericItemsMapper):
    SQL_PARAMS = {"values": [6678]}


class AmdsDiastolicBloodPressureNonInvasiveMapper(AbstractAmdsNumericItemsMapper):
    SQL_PARAMS = {"values": [6680]}


class AmdsMeanBloodPressureNonInvasiveMapper(AbstractAmdsNumericItemsMapper):
    SQL_PARAMS = {"values": [6679]}
