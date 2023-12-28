from pathlib import Path

import pandas as pd

from icu_pipeline.mapper.schema import AbstractSinkSchema
from icu_pipeline.mapper.sink import AbstractSinkMapper


class CSVFileSinkMapper(AbstractSinkMapper):
    def __init__(self, path: Path | None = None) -> None:
        super().__init__()

        self._path = path or Path(".")

    def to_output_format(
        self,
        df: pd.DataFrame,
        schema: AbstractSinkSchema,
        id: str,
    ) -> None:
        file_path = self._path / f"{schema._SINK_NAME}" / f"{id}.csv"

        header = False
        if not file_path.exists():
            header = True

        file_path.parent.mkdir(parents=True, exist_ok=True)

        for column in df.columns:
            if not isinstance(df[column][0], dict):
                continue

            df = df.join(pd.json_normalize(df[column]).add_prefix(f"{column}__"))  # type: ignore[arg-type]
            df = df.drop(columns=[column])

        df.columns = df.columns.str.replace(".", "__")

        df.to_csv(file_path, mode="a+", index=False, header=header)
