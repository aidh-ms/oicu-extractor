from abc import ABCMeta

import pandas as pd
from pandera.typing import DataFrame

from icu_pipeline.mapper.schema.fhir import (
    Quantity,
)
from icu_pipeline.mapper.schema.fhir.observation import FHIRObservation
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


class MeanArterialBloodPressureNonInvasiveMapper(AbstractMimicChartEventsMapper):
    SQL_PARAMS = {"values": [220181]}


class OxygenSaturationMapper(AbstractMimicChartEventsMapper):
    SQL_PARAMS = {"values": [220277]}


class TemperatureMapper(AbstractMimicChartEventsMapper):
    SQL_PARAMS = {"values": [226329, 223762]}


class TemperatureFahrenheitMapper(AbstractMimicChartEventsMapper):
    SQL_PARAMS = {"values": [223761]}

    def _to_fihr(self, df: DataFrame) -> DataFrame[FHIRObservation]:
        observation_df = super()._to_fihr(df)
        observation_df[FHIRObservation.value_quantity] = observation_df[
            FHIRObservation.value_quantity
        ].map(
            lambda quantity: Quantity(value=(quantity["value"] - 32) * 5 / 9, unit="°C")
        )

        return observation_df.pipe(DataFrame[FHIRObservation])


class HeightMapper(AbstractMimicChartEventsMapper):
    SQL_PARAMS = {"values": [226730]}


class WeightMapper(AbstractMimicChartEventsMapper):
    SQL_PARAMS = {"values": [226512]}
