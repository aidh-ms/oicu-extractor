import pytest
import pandas as pd

from unittest.mock import patch

from pandas import Timestamp

from icu_pipeline.schema.fhir import (
    FHIRObservation,
)
from icu_pipeline.source.mimiciv import MimicObservationMapper


class TestObservationMapper:
    @pytest.fixture
    def mapper(self):
        with patch(
            "icu_pipeline.source.database.AbstractDatabaseSourceMapper.build_query",
            return_value="",
        ):
            yield MimicObservationMapper(
                schema="mimiciv_icu",
                table="chartevents",
                constraints={"itemid": "220045"},
                concept_id="364075005",
                concept_type="snomed",
                source_config="test"
            )

    def test_get_data(self, mapper: MimicObservationMapper):
        """
        Test for the get_data method of the AbstractDatabaseSourceMapper class.
        Asserts that the method raises NotImplementedError.
        """

        df = pd.DataFrame(
            {
                "patient_id": [
                    1234,
                    1234,
                    1234,
                    1234,
                    1234,
                    1234,
                    1234,
                    1234,
                    1234,
                    1234,
                ],
                "timestamp": [
                    Timestamp("2173-08-03 16:00:00"),
                    Timestamp("2173-08-03 17:00:00"),
                    Timestamp("2173-08-03 18:00:00"),
                    Timestamp("2173-08-04 07:00:00"),
                    Timestamp("2173-08-04 08:00:00"),
                    Timestamp("2173-08-04 09:00:00"),
                    Timestamp("2173-08-04 10:00:00"),
                    Timestamp("2173-08-04 11:00:00"),
                    Timestamp("2173-08-04 12:01:00"),
                    Timestamp("2173-08-04 13:00:00"),
                ],
                "value": [
                    107.0,
                    108.0,
                    101.0,
                    96.0,
                    95.0,
                    92.0,
                    92.0,
                    90.0,
                    91.0,
                    86.0,
                ],
                "unit": [
                    "bpm",
                    "bpm",
                    "bpm",
                    "bpm",
                    "bpm",
                    "bpm",
                    "bpm",
                    "bpm",
                    "bpm",
                    "bpm",
                ],
            }
        )

        observation_df = mapper._to_fihr(df)
        assert observation_df is not None
        assert len(observation_df.index) == 10
        FHIRObservation.validate(observation_df)  # will raise an exception if invalid

        assert observation_df[FHIRObservation.subject][0]["reference"] == "1234"
        assert observation_df[FHIRObservation.subject][0]["type"] == "mimic"
        assert observation_df[FHIRObservation.effective_date_time][0] == Timestamp(
            "2173-08-03 16:00:00", tz="UTC"
        )
        assert observation_df[FHIRObservation.value_quantity][0]["value"] == 107.0
        assert observation_df[FHIRObservation.value_quantity][0]["unit"] == "bpm"
        assert observation_df[FHIRObservation.code][0]["coding"]["code"] == "364075005"
        assert observation_df[FHIRObservation.code][0]["coding"]["system"] == "snomed"
