from pandera.typing import DataFrame

from icu_pipeline.mapper.schema.fhir import (
    Quantity,
)
from icu_pipeline.mapper.schema.fhir.observation import FHIRObservation
from icu_pipeline.mapper.source.mimic import AbstractMimicEventsMapper


class MimicTemperatureFahrenheitMapper(AbstractMimicEventsMapper):
    SQL_PARAMS = {"values": [223761]}

    def _to_fihr(self, df: DataFrame) -> DataFrame[FHIRObservation]:
        observation_df = super()._to_fihr(df)
        observation_df[FHIRObservation.value_quantity] = observation_df[
            FHIRObservation.value_quantity
        ].map(
            lambda quantity: Quantity(
                value=(quantity["value"] - 32) * 5 / 9, unit="°C")
        )

        return observation_df.pipe(DataFrame[FHIRObservation])
