import pytest
from pandas import DataFrame
from sqlalchemy.engine import create_engine
from icu_pipeline.mapper.source import AbstractDatabaseSourceMapper, SourceMapperConfiguration


class DummyDatabaseSourceMapper(AbstractDatabaseSourceMapper):
    limit = -1

    def create_connection(self):
        return create_engine("sqlite:///test_mimiciv.sqlite").connect().execution_options(stream_results=True)

    def _to_fihr(self, df: DataFrame) -> DataFrame:
        return df


class DummySourceMapperConfiguration(SourceMapperConfiguration):
    limit = -1


class TestDatabaseSourceMapper:
    @pytest.fixture
    def mapper(self):
        concept_id = "HeartRate"
        concept_type = "VitalSign"
        fhir_schema = None
        mapper = DummyDatabaseSourceMapper(
            concept_id=concept_id, concept_type=concept_type, fhir_schema=fhir_schema, source_config=DummySourceMapperConfiguration())
        table = "chartevents"
        item_ids = "1, 2"
        mapper.SQL_QUERY = f"SELECT * FROM {
            table} WHERE itemid IN ({item_ids})"
        return mapper

    # def test_get_data(self, mapper):
    #     """
    #     Test for the get_data method of the AbstractDatabaseSourceMapper class.
    #     Asserts that the method raises NotImplementedError.
    #     """
    #     df = mapper.get_data()  # type: ignore
    #     # unpack the generator
    #     df = next(df)
    #     assert df is not None
    #     assert df.shape[1] == 2
    #     assert df.shape[0] > 0

    # def test_get_data_with_limit(self, mapper):
    #     """
    #     Test for the get_data method of the AbstractDatabaseSourceMapper class.
    #     Asserts that the method raises NotImplementedError.
    #     """
    #     df = mapper.get_data(limit=1)
    #     assert df is not None
    #     assert df.shape[1] == 1
    #     assert df.shape[0] > 0
