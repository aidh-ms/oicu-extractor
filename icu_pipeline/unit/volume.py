from typing import Callable
from pandera.typing import Series, DataFrame
from icu_pipeline.schema.fhir import Quantity
from icu_pipeline.unit.converter import BaseConverter


class LengthConverter(BaseConverter):
    SI_UNIT = "l"
    AVAILABLE_UNITS = ["l", "ml"]

    def _convertToSI(
        self,
        source_unit: str,
        data: Series[Quantity],
        dependencies: dict[str, DataFrame],
    ):
        convert: Callable[[float], float] = lambda v: v
        # Data can have any Unit and will be transformed to m
        match source_unit:
            # Already SI-Unit
            case self.SI_UNIT:
                return data

            # Actual Conversions from case to cm
            case "ml":
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
        # Data uses m and can be transformed in to any Unit
        match sink_unit:
            # Already SI-Unit
            case self.SI_UNIT:
                return data

            # Actual Conversions
            case "ml":
                convert = lambda v: v / 100

            # Not Implemented
            case _:
                raise NotImplementedError

        return data.apply(lambda q: Quantity(value=convert(q["value"]), unit=sink_unit))
