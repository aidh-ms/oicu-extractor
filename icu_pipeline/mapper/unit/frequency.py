from pandera.typing import Series
from icu_pipeline.mapper.schema.fhir import Quantity
from icu_pipeline.mapper.unit.converter import BaseConverter


class FrequencyConverter(BaseConverter):
    SI_UNIT = "Hz"
    AVAILABLE_UNITS = ["Hz", "bpm"]

    def _convertToSI(self, data: Series[Quantity]):
        convert = lambda v: v
        # Data can use any unit and will be transformed to Hz
        match self._source:
            # Already SI-Unit
            case self.SI_UNIT:
                return data

            # Actual Conversions
            case "bpm":
                convert = lambda v: v / 60 # bpm = 60 * Hz

            # Not Implemented
            case _:
                raise NotImplementedError

        return data.apply(lambda q: Quantity(
            value=convert(q["value"]),
            unit=self.SI_UNIT))

    def _convertToTarget(self, data: Series[Quantity]):
        convert = lambda v: v
        # Data contains Hz values and can be transformed into any Unit
        match self._target:
            # Already SI-Unit
            case self.SI_UNIT:
                return data

            # Actual Conversions
            case "bpm":
                convert = lambda v: v * 60 # 60 Seconds in 1 Minute

            # Not Implemented
            case _:
                raise NotImplementedError

        return data.apply(lambda q: Quantity(
            value=convert(q["value"]),
            unit=self._target))
