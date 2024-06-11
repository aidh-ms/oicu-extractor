from typing import Callable
from pandera.typing import Series
from icu_pipeline.mapper.schema.fhir import Quantity
from icu_pipeline.mapper.unit.converter import BaseConverter


class PressureConverter(BaseConverter):
    SI_UNIT = "Pa" # Short name for Kg*m/s**2
    AVAILABLE_UNITS = ["Pa", "mmHg", "bar", "mbar"]

    def _convertToSI(self, data: Series[Quantity]):
        convert: Callable[[float], float] = lambda v: v
        # Data can have any Unit and will be transformed to °C
        match self._source:
            # Already SI-Unit
            case self.SI_UNIT:
                return data

            # Actual Conversions
            case "mmHg":
                convert = lambda v: v * 133.322 # 1 mmHg ~= 133.322 Pa

            case "bar":
                convert = lambda v: v * 10e5

            case "mbar":
                convert = lambda v: v * 10e2

            # Not Implemented
            case _:
                raise NotImplementedError

        return data.apply(lambda q: Quantity(
            value=convert(q["value"]),
            unit=self.SI_UNIT))

    def _convertToTarget(self, data: Series[Quantity]):
        convert: Callable[[float], float] = lambda v: v
        # Data uses °C and can be transformed in to any Unit
        match self._target:
            # Already SI-Unit
            case self.SI_UNIT:
                return data

            # Actual Conversions
            case "mmHg":
                convert = lambda v: v / 133.322 # 1 mmHg ~= 133.322 Pa

            case "bar":
                convert = lambda v: v / 10e5

            case "mbar":
                convert = lambda v: v / 10e2

            # Not Implemented
            case _:
                raise NotImplementedError

        return data.apply(lambda q: Quantity(
            value=convert(q["value"]),
            unit=self.SI_UNIT))