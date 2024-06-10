from typing import Generator
from pandera.typing import Series, DataFrame
from icu_pipeline.mapper.schema.fhir import Quantity


class BaseConverter:
    SI_UNIT = None
    AVAILABLE_UNITS = []
    REQUIRED_PARAMETERS = [] # TODO - Unit Conversion might need other paramters like height, weight, ...

    def __init__(self, source: str, target: str|None = None) -> None:
        self._source = source
        self._target = target if target is not None else self.SI_UNIT

    def convert(self, data: Generator[DataFrame, None, None]) -> Generator[DataFrame, None, None]:
        for d in data:
            # Check if output != input
            if self._source == self._target:
                yield d

            # FHIR Quantities need conversion of column 'value_quantity'
            if "value_quantity" in d.columns:
                # Convert inplace
                self._convertToSI(d["value_quantity"])
                self._convertToTarget(d["value_quantity"])
            # Yield optionally converted DF
            yield d

    def _convertToSI(self, data: Series[Quantity]):
        raise NotImplementedError

    def _convertToTarget(self, data: Series[Quantity]):
        raise NotImplementedError

    @staticmethod
    def getConverter(source: str, target: str|None = None):
        relevant_subclass = None
        for next_subclass in BaseConverter.__subclasses__():
            if source in next_subclass.AVAILABLE_UNITS or target in next_subclass.AVAILABLE_UNITS:
                relevant_subclass = next_subclass
                break
        assert relevant_subclass is not None, f"No Converter found containing any of ['{source}', '{target}']"
        assert source in relevant_subclass.AVAILABLE_UNITS, f"Converter '{relevant_subclass.__name__}' can't handle source unit '{source}'"
        if target is not None:
            assert target in relevant_subclass.AVAILABLE_UNITS, f"Converter '{relevant_subclass.__name__}' can't handle target unit '{source}'"

        return relevant_subclass(source, target)
