from abc import ABC, abstractmethod
from importlib import import_module
from typing import TYPE_CHECKING, Generator, Generic, TypeVar

from pandera.typing import DataFrame

from conceptbase.config import DataSource, MapperConfig, SourceConfig
from icu_pipeline.logger import ICULogger
from icu_pipeline.schema.fhir import AbstractFHIRSinkSchema

if TYPE_CHECKING:
    from icu_pipeline.job import Job

# add logging
logger = ICULogger.get_logger()


######
# Abstract SourceMapper and SourceSampler
######
F = TypeVar("F", bound=AbstractFHIRSinkSchema)


class AbstractSourceMapper(ABC, Generic[F]):
    """
    Abstract class for the source mappers.

    This class is used to map data from a source to a sink. It provides a base structure for
    specific source mappers, which should implement the `map` method.

    Parameters
    ----------
    concept_id : str
        The concept ID to be used in the mapping process.
    concept_type : str
        The type of the concept to be used in the mapping process.
    fhir_schema : Type[AbstractFHIRSinkSchema]
        The FHIR schema to be used in the mapping process.
    source_config : SourceConfiguration
        The configuration for the source mapper.

    Methods
    -------
    map():
        Maps the data from the source to the sink. This method should be implemented by subclasses.
    """

    def __init__(
        self,
        concept_id: str,
        concept_type: str,
        fhir_schema: type[AbstractFHIRSinkSchema] | str,
        datasource: DataSource,
        source_config: SourceConfig,
        unit: str,
    ) -> None:
        super().__init__()

        self._concept_id = concept_id
        self._concept_type = concept_type
        self._fhir_schema = fhir_schema
        if isinstance(fhir_schema, str):
            module = import_module("icu_pipeline.schema.fhir")
            self.fhir_schema: type[AbstractFHIRSinkSchema] = getattr(module, fhir_schema)
        self._data_source = datasource
        self._source_config = source_config
        self._unit = unit

    @abstractmethod
    def get_data(self, job: "Job") -> DataFrame:
        """
        Retrieves the data to be mapped.

        This method should be implemented by subclasses.

        Returns
        -------
        Generator
            A generator that yields dataframes with the data to be mapped.
        """
        raise NotImplementedError

    @abstractmethod
    def _to_fihr(self, df: DataFrame) -> DataFrame[F]:
        """
        Converts a dataframe to FHIR format.

        This method should be implemented by subclasses.

        Parameters
        ----------
        df : DataFrame
            The dataframe to be converted.

        Returns
        -------
        DataFrame
            The dataframe in FHIR format.
        """
        raise NotImplementedError


class AbstractSourceSampler(ABC):
    """
    Abstract class for the source samplers.
    Data-Queries are chuncked into subsets of samples. Samplers are supposed to create a list of patient identifiers.

    Methods
    -------
    get_samples():
        Create a Generator, which produces Sample IDs
    """

    IDENTIFIER: list[str] = []

    def __init__(self) -> None:
        assert (
            self.IDENTIFIER is not None and len(self.IDENTIFIER) > 0
        ), f"Class {type(self)} has no Identifiers defined."

    @abstractmethod
    def get_samples(self) -> Generator[DataFrame, None, None]:
        pass


#######
# Getter
#######


def getDataSourceMapper(config: MapperConfig) -> type[AbstractSourceMapper]:
    """Load the SourceMapper from the corresponding module."""
    module = import_module(f"icu_pipeline.source.{config.source}")
    source_mapper = getattr(module, config.klass)
    return source_mapper  # type: ignore[no-any-return]


def getDataSampler(source: DataSource, source_config: SourceConfig) -> AbstractSourceSampler:
    match source:
        case DataSource.MIMICIV:
            from icu_pipeline.source.mimiciv import MimicSampler

            return MimicSampler(source_config)
        case DataSource.EICU:
            from icu_pipeline.source.eicu import EICUSampler

            return EICUSampler(source_config)
        case _:
            raise NotImplementedError
