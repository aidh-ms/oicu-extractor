from pandera.typing import Series
from icu_pipeline.mapper.schema.fhir import Quantity
from icu_pipeline.mapper.unit.converter import BaseConverter


class TemperatureConverter(BaseConverter):
    SI_UNIT = "°C"
    AVAILABLE_UNITS = ["°C", "°F", "°K"]

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    # def convert(self, data):
    #     raise NotImplementedError

    def _convertToSI(self, data: Series[Quantity]):
        convert = lambda v: v
        # Data can have any Unit and will be transformed to °C
        match self._source:
            # Already SI-Unit
            case self.SI_UNIT:
                return data

            # Actual Conversions
            case "°F":
                convert = lambda v: (v - 32) * 5 / 9

            case "°K":
                convert = lambda v: v - 273.15

            # Not Implemented
            case _:
                raise NotImplementedError

        return data.apply(lambda q: Quantity(
            value=convert(q["value"]),
            unit=self.SI_UNIT))

    def _convertToTarget(self, data: Series[Quantity]):
        convert = lambda v: v
        # Data uses °C and can be transformed in to any Unit
        match self._target:
            # Already SI-Unit
            case self.SI_UNIT:
                return data

            # Actual Conversions
            case "°F":
                convert = lambda v: (v * 9 / 5) + 32

            case "°K":
                convert = lambda v: v + 273.15

            # Not Implemented
            case _:
                raise NotImplementedError

        return data.apply(lambda q: Quantity(
            value=convert(q["value"]),
            unit=self.SI_UNIT))