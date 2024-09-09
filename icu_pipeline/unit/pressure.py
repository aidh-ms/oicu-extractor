from typing import Callable
from pandera.typing import Series, DataFrame
from icu_pipeline.schema.fhir import Quantity
from icu_pipeline.unit.converter import BaseConverter


class PressureConverter(BaseConverter):
    SI_UNIT = "Pa" # Short name for Kg*m/s**2
    AVAILABLE_UNITS = ["Pa", "mmHg", "bar", "mbar", "cmH2O"]

    def _convertToSI(self, source_unit: str, data: Series[Quantity], dependencies: dict[str,DataFrame]):
        convert: Callable[[float], float] = lambda v: v
        # Data can have any Unit and will be transformed to °C
        match source_unit:
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

            case "cmH2O":
                convert = lambda v: v * 98.0665

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
            case "mmHg":
                convert = lambda v: v / 133.322 # 1 mmHg ~= 133.322 Pa

            case "bar":
                convert = lambda v: v / 10e5

            case "mbar":
                convert = lambda v: v / 10e2

            case "cmH2O":
                convert = lambda v: v / 98.0665

            # Not Implemented
            case _:
                raise NotImplementedError

        return data.apply(lambda q: Quantity(
            value=convert(q["value"]),
            unit=sink_unit))