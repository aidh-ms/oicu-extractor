import pandas as pd
import pytest
from unittest.mock import Mock, patch
from pathlib import Path
from icu_pipeline.mapper.sink.file import CSVFileSinkMapper, JSONLFileSinkMapper


class TestCSVFileSinkMapper:
    @pytest.fixture
    def mock_df_generator(self):
        df = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]})
        return (x for x in [df])

    @pytest.fixture
    def mock_concept(self):
        mock_concept = Mock()
        mock_concept._concept_config.name = "TestConcept"
        return mock_concept

    @patch("pandas.DataFrame.to_csv")
    def test_to_output_format(self, mock_to_csv, mock_df_generator, mock_concept):
        mapper = CSVFileSinkMapper(Path("."))
        result = mapper.to_output_format(mock_df_generator, mock_concept)

        assert result["total_rows"] == 3
        mock_to_csv.assert_called()


class TestJSONLFileSinkMapper:
    @patch("pandas.DataFrame.to_json")
    def test_to_output_format(self, mock_to_json):
        mapper = JSONLFileSinkMapper(Path("."))
        df = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]})

        with pytest.raises(NotImplementedError):
            mapper._to_output_format(df)
