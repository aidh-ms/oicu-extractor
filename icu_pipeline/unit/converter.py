from dataclasses import dataclass
from pandas.core.api import DataFrame as DataFrame
from pandera.typing import Series
from ..job import Job
from icu_pipeline.schema.fhir import Quantity
from icu_pipeline.graph import Node
from icu_pipeline.source import DataSource


@dataclass
class ConverterConfig:
    concept_id: str
    source_units: dict[DataSource, str]
    sink_unit: str | None


class BaseConverter(Node):
    SI_UNIT = None
    AVAILABLE_UNITS: list[str] = []

    def __init__(self, converter_config: ConverterConfig) -> None:
        super().__init__(concept_id=converter_config.concept_id)
        self._config = converter_config
        if self._config.sink_unit is None:
            self._config.sink_unit = self.SI_UNIT

    def get_data(self, job: Job, *args, **kwargs) -> DataFrame:
        expected_sources = 1 + len(self.REQUIRED_CONCEPTS)
        n_sources = len(self._sources)
        assert (
            expected_sources == n_sources
        ), f"Converters error. Expected {expected_sources} but found {n_sources} sources (of which dependencies: {len(self.REQUIRED_CONCEPTS)})"
        assert (
            job.database in self._config.source_units
        ), f"DataSource '{job.database}' is not configured for this Converter."

        # Read all sources
        data = super().fetch_sources(job, *args, **kwargs)

        # If In-Unit == Out-Unit
        if self._config.source_units[job.database] == self._config.sink_unit:
            return data[self._concept_id]

        # Otherwise use the converter methods
        converted_data = self.convert(
            source_unit=self._config.source_units[job.database], sink_unit=self._config.sink_unit, data=data
        )
        return converted_data[self._concept_id]

    def convert(self, source_unit: str, sink_unit: str, data: dict[str, DataFrame]) -> DataFrame:
        # Check if output != input
        relevant_data = data[self._concept_id]

        # FHIR Quantities need conversion of column 'value_quantity'
        if "value_quantity" in relevant_data.columns:
            # Convert inplace
            relevant_data["value_quantity"] = self._convertToSI(source_unit, relevant_data["value_quantity"], data)
            relevant_data["value_quantity"] = self._convertToTarget(sink_unit, relevant_data["value_quantity"], data)

        return data

    def _convertToSI(self, source_unit: str, data: Series[Quantity], dependencies: dict[str, DataFrame]):
        raise NotImplementedError

    def _convertToTarget(self, sink_unit: str, data: Series[Quantity], dependencies: dict[str, DataFrame]):
        raise NotImplementedError

    @staticmethod
    def getConverter(config: ConverterConfig):
        relevant_subclass = None
        # Check all implemented Subclasses
        for next_subclass in BaseConverter.__subclasses__():
            # If the default unit for this concept (see config) is available, use this one
            if config.sink_unit in next_subclass.AVAILABLE_UNITS:
                relevant_subclass = next_subclass
                break
        assert relevant_subclass is not None, f"No Converter found for config {config}"
        # Make sure that the source units are also implemented
        for source in config.source_units.values():
            assert (
                source in relevant_subclass.AVAILABLE_UNITS
            ), f"Converter '{relevant_subclass.__name__}' can't handle source unit '{source}'"

        return relevant_subclass(converter_config=config)
