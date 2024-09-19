from typing import Callable
from pandera.typing import Series, DataFrame
from icu_pipeline.schema.fhir import Quantity
from icu_pipeline.unit.converter import BaseConverter


class DistanceConverter(BaseConverter):
    SI_UNIT = "m"
    AVAILABLE_UNITS = [
        "cm"
    ]  # year is amiguous (365, 366 days). TODO - Ignore since error is small and mimic does it anyway?
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
            case "cm":
                convert = lambda v: v * 100

            # Not Implemented
            case _:
                raise NotImplementedError

        return data.apply(
            lambda q: Quantity(value=convert(q["value"]), unit=self.SI_UNIT)
        )

    def _convertToTarget(
        self, sink_unit: str, data: Series[Quantity], dependencies: dict[str, DataFrame]
    ):
        convert: Callable[[float], float] = lambda v: v
        # Data contains Hz values and can be transformed into any Unit
        match sink_unit:
            # Already SI-Unit
            case self.SI_UNIT:
                return data

            # Actual Conversions
            case "cm":
                convert = lambda v: v / 100

            # Not Implemented
            case _:
                raise NotImplementedError

        return data.apply(lambda q: Quantity(value=convert(q["value"]), unit=sink_unit))
