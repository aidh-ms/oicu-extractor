from typing import Callable
from pandera.typing import Series, DataFrame
from icu_pipeline.schema.fhir import Quantity
from icu_pipeline.unit.converter import BaseConverter


class TemperatureConverter(BaseConverter):
    SI_UNIT = "°K"
    AVAILABLE_UNITS = ["°K", "°C", "°F"]
    REQUIRED_CONCEPTS = []

    def _convertToSI(self, source_unit: str,data: Series[Quantity], dependencies: dict[str,DataFrame]):
        convert: Callable[[float], float] = lambda v: v
        # Data can have any Unit and will be transformed to °C
        match source_unit:
            # Already SI-Unit
            case self.SI_UNIT:
                return data

            # Actual Conversions
            case "°C":
                convert = lambda v: v + 273.15

            case "°F":
                convert = lambda v: (v - 32) * 5 / 9 + 273.15

            # Not Implemented
            case _:
                raise NotImplementedError

        return data.apply(lambda q: Quantity(
            value=convert(q["value"]),
            unit=self.SI_UNIT))

    def _convertToTarget(self, sink_unit: str, data: Series[Quantity], dependencies: dict[str,DataFrame]):
        convert: Callable[[float], float] = lambda v: v
        # Data uses °C and can be transformed in to any Unit
        match sink_unit:
            # Already SI-Unit
            case self.SI_UNIT:
                return data

            # Actual Conversions
            case "°C":
                convert = lambda v: v - 273.15

            case "°F":
                convert = lambda v: ((v - 273.15) * 9 / 5) + 32

            # Not Implemented
            case _:
                raise NotImplementedError

        return data.apply(lambda q: Quantity(
            value=convert(q["value"]),
            unit=self.SI_UNIT))