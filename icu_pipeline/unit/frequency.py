from typing import Callable
from pandera.typing import Series, DataFrame
from icu_pipeline.schema.fhir import Quantity
from icu_pipeline.unit.converter import BaseConverter


class FrequencyConverter(BaseConverter):
    SI_UNIT = "Hz"
    AVAILABLE_UNITS = ["Hz", "bpm", "1/min"]
    # REQUIRED_CONCEPTS = ["SystolicBloodPressure"]

    def _convertToSI(
        self,
        source_unit: str,
        data: Series[Quantity],
        dependencies: dict[str, DataFrame],
    ):
        convert: Callable[[float], float] = lambda v: v
        # Data can use any unit and will be transformed to Hz
        match source_unit:
            # Already SI-Unit
            case self.SI_UNIT:
                return data

            # Actual Conversions
            case "bpm":
                convert = lambda v: v / 60  # bpm = 60 * Hz

            case "1/min":
                convert = lambda v: v / 60

            # Not Implemented
            case _:
                raise NotImplementedError

        return data.apply(lambda q: Quantity(value=convert(q["value"]), unit=self.SI_UNIT))

    def _convertToTarget(self, sink_unit: str, data: Series[Quantity], dependencies: dict[str, DataFrame]):
        convert: Callable[[float], float] = lambda v: v
        # Data contains Hz values and can be transformed into any Unit
        match sink_unit:
            # Already SI-Unit
            case self.SI_UNIT:
                return data

            # Actual Conversions
            case "bpm":
                convert = lambda v: v * 60  # 60 Seconds in 1 Minute

            case "1/min":
                convert = lambda v: v * 60

            # Not Implemented
            case _:
                raise NotImplementedError

        return data.apply(lambda q: Quantity(value=convert(q["value"]), unit=sink_unit))
