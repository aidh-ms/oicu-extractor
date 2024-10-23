import pandera as pa


class AbstractSinkSchema(pa.DataFrameModel):
    """
    Abstract class for the sink schema.
    """

    _SINK_NAME: str
