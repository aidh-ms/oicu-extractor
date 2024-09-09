from abc import ABCMeta
from pathlib import Path
from typing import Generator
import pandas as pd

from icu_pipeline.sink import AbstractSinkMapper
from icu_pipeline.concept import Concept


class AbstractFileSinkMapper(AbstractSinkMapper, metaclass=ABCMeta):
    """
    A class used to map data to a CSV file.

    This class inherits from the AbstractFileSinkMapper and overrides the
    `to_output_format` method to write data to a CSV file.

    ...

    Attributes
    ----------
    FILE_EXTENSION : str
        The file extension for the output file. This is always "csv" for this class.

    Methods
    -------
    __init__(self, path: Path | None = None) -> None:
        Initializes the CSVFileSinkMapper.

    to_output_format(
        self,
        df_generator: Generator[pd.DataFrame, None, None],
        concept: Concept,
    ) -> None:
        Writes data from a generator of pandas DataFrames to a CSV file.

    """
    FILE_EXTENSION: str

    def __init__(self, path: Path | None = None) -> None:
        """
        Initializes the AbstractFileSinkMapper.

        Parameters
        ----------
        path : Path | None, optional
            The path where the output file should be written, by default None

        """
        super().__init__()

        self._path = path or Path("./output")
        self._path.mkdir(exist_ok=True)

    def to_output_format(
        self,
        df: pd.DataFrame,
        concept: Concept,
    ) -> None:
        raise NotImplementedError


class CSVFileSinkMapper(AbstractFileSinkMapper):
    """
    A class used to map data to a CSV file.

    This class inherits from the AbstractFileSinkMapper and overrides the
    `to_output_format` method to write data to a CSV file.

    ...

    Attributes
    ----------
    FILE_EXTENSION : str
        The file extension for the output file. This is always "csv" for this class.

    Methods
    -------
    __init__(self, path: Path | None = None) -> None:
        Initializes the CSVFileSinkMapper.

    to_output_format(
        self,
        df_generator: Generator[pd.DataFrame, None, None],
        concept: Concept,
    ) -> None:
        Writes data from a generator of pandas DataFrames to a CSV file.

    """

    FILE_EXTENSION = "csv"

    def __init__(self, path: Path | None = None) -> None:
        """
        Initializes the CSVFileSinkMapper.

        Parameters
        ----------
        path : Path | None, optional
            The path where the output file should be written, by default None

        """
        super().__init__(path)

    def to_output_format(
        self,
        df_generator: Generator[pd.DataFrame, None, None],
        concept: Concept,
    ) -> None:
        """
        Writes data from a generator of pandas DataFrames to a CSV file.

        The file is named after the concept's name with the CSV extension. If the file
        already exists, the data is appended to the file. If the file does not exist,
        a new file is created.

        Parameters
        ----------
        df_generator : Generator[pd.DataFrame, None, None]
            A generator that yields pandas DataFrames.
        concept : Concept
            The concept that the data represents.

        Returns
        -------
        dict
            A dictionary with a single key-value pair. The key is "total_rows" and the
            value is the total number of rows written to the file.

        """
        out = dict(total_rows=0)
        for df in df_generator:
            header = False
            file_path = self._path / \
                f"{concept._concept_config.name}.{self.FILE_EXTENSION}"
            if not file_path.exists():
                header = True

            for column in df.columns:
                if not isinstance(df[column][0], dict):
                    continue

                df = df.join(pd.json_normalize(df[column]).add_prefix(
                    f"{column}__"))  # type: ignore[arg-type]
                df = df.drop(columns=[column])

            df.columns = df.columns.str.replace(".", "__")

            # TODO - Some basic statistics. Maybe more details?
            out["total_rows"] += len(df)
            df.to_csv(file_path, mode="a+", index=False, header=header)
        return out


class JSONLFileSinkMapper(AbstractFileSinkMapper):
    """
    A class used to map data to a JSONL file.

    This class inherits from the AbstractFileSinkMapper and is intended to override
    the `to_output_format` method to write data to a JSONL file. However, currently
    this method is not implemented.

    ...

    Attributes
    ----------
    FILE_EXTENSION : str
        The file extension for the output file. This is always "jsonl" for this class.

    Methods
    -------
    _to_output_format(
        self,
        df: pd.DataFrame,
    ) -> None:
        Intended to write data from a pandas DataFrame to a JSONL file. Currently not implemented.

    """
    FILE_EXTENSION = "jsonl"

    def _to_output_format(
        self,
        df: pd.DataFrame,
    ) -> None:
        """
        Intended to write data from a pandas DataFrame to a JSONL file. Currently not implemented.

        Parameters
        ----------
        df : pd.DataFrame
            A pandas DataFrame containing the data to be written to the file.

        Raises
        ------
        NotImplementedError
            This method is not yet implemented.

        """
        raise NotImplementedError
        df.to_json(self._path, mode="a", orient="records", lines=True)
