from typing import Callable

from pandera.typing import DataFrame, Series

from icu_pipeline.schema.fhir import Quantity
from icu_pipeline.unit.converter import BaseConverter


class NoUnitConverter(BaseConverter):
    SI_UNIT = ""
    AVAILABLE_UNITS = [
        ""
    ]  # year is amiguous (365, 366 days). TODO - Ignore since error is small and mimic does it anyway?
    # REQUIRED_CONCEPTS = ["SystolicBloodPressure"]

    def _convertToSI(
        self,
        source_unit: str,
        data: Series[Quantity],  # type: ignore[type-var]
        dependencies: dict[str, DataFrame],
    ) -> Series:
        convert: Callable[[float], float] = lambda v: v
        # Data can use any unit and will be transformed to Hz
        match source_unit:
            # Already SI-Unit
            case self.SI_UNIT:
                return data

            # Actual Conversions
            case "":
                convert = lambda v: v

            # Not Implemented
            case _:
                raise NotImplementedError

        return data.apply(lambda q: Quantity(value=convert(q["value"]), unit=self.SI_UNIT)).pipe(Series)

    def _convertToTarget(
        self,
        sink_unit: str,
        data: Series[Quantity],  # type: ignore[type-var]
        dependencies: dict[str, DataFrame],
    ) -> Series:
        convert: Callable[[float], float] = lambda v: v
        # Data contains Hz values and can be transformed into any Unit
        match sink_unit:
            # Already SI-Unit
            case self.SI_UNIT:
                return data

            # Actual Conversions
            case "":
                convert = lambda v: v

            # Not Implemented
            case _:
                raise NotImplementedError

        return data.apply(lambda q: Quantity(value=convert(q["value"]), unit=sink_unit)).pipe(Series)
