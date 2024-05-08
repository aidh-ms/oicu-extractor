from abc import ABCMeta
from pathlib import Path
from typing import Generator
import pandas as pd

from icu_pipeline.mapper.sink import AbstractSinkMapper
from icu_pipeline.concept import Concept


class AbstractFileSinkMapper(AbstractSinkMapper, metaclass=ABCMeta):
    FILE_EXTENSION: str

    def __init__(self, path: Path | None = None) -> None:
        super().__init__()

        self._path = path or Path(".")
        self._path.mkdir(exist_ok=True)

    def to_output_format(
        self,
        df: pd.DataFrame,
        concept: Concept,
        # file_path: Path, # Don't explicitly give a Path. It should be defined on init
    ) -> None:
        raise NotImplementedError


class CSVFileSinkMapper(AbstractFileSinkMapper):
    FILE_EXTENSION = "csv"

    def __init__(self, path: Path | None = None) -> None:
        super().__init__(path)

    def to_output_format(
        self,
        df_generator: Generator[pd.DataFrame, None, None],
        concept: Concept,
    ) -> None:
        out = dict(total_rows=0)
        for df in df_generator:
            header = False
            file_path = self._path / f"{concept._concept_config.id}.{self.FILE_EXTENSION}"
            if not file_path.exists():
                header = True

            for column in df.columns:
                if not isinstance(df[column][0], dict):
                    continue

                df = df.join(pd.json_normalize(df[column]).add_prefix(f"{column}__"))  # type: ignore[arg-type]
                df = df.drop(columns=[column])

            df.columns = df.columns.str.replace(".", "__")

            # TODO - Some basic statistics. Maybe more details?
            out["total_rows"] += len(df)
            df.to_csv(file_path, mode="a+", index=False, header=header)
        return out


class JSONLFileSinkMapper(AbstractFileSinkMapper):
    FILE_EXTENSION = "jsonl"

    def _to_output_format(
        self,
        df: pd.DataFrame,
    ) -> None:
        raise NotImplementedError
        df.to_json(self._path, mode="a", orient="records", lines=True)
