from abc import ABCMeta
import pandas as pd

from pandera.typing import DataFrame

from icu_pipeline.mapper.schema.fhir import (
    Quantity,
)

from icu_pipeline.mapper.schema.fhir.observation import FHIRObservation
from icu_pipeline.mapper.source.amds import AbstractAmdsEventsMapper


class AbstractAmdsNumericItemsMapper(AbstractAmdsEventsMapper, metaclass=ABCMeta):
    SQL_QUERY = (
        "SELECT * FROM amsterdamumcdb.numericitems WHERE itemid = any(%(values)s);"
    )


class AmdsHeartRateMapper(AbstractAmdsNumericItemsMapper):
    SQL_PARAMS = {"values": [6640]}
