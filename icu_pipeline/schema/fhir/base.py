from typing import TypedDict, Annotated

import pandas as pd
from pydantic import PlainValidator

from icu_pipeline.schema import AbstractSinkSchema


class Reference(TypedDict):
    """
    A TypedDict representing a FHIR Reference.

    Attributes
    ----------
    reference : str
        The actual reference to the resource.
    type : str
        The type of the referenced resource.
    """

    reference: str
    type: str


class Quantity(TypedDict):
    """
    A TypedDict representing a FHIR Quantity.

    Attributes
    ----------
    value : float
        The numerical value of the quantity.
    unit : str
        The unit of the quantity.
    """

    value: float
    unit: str


class Period(TypedDict):
    """
    A TypedDict representing a FHIR Period.

    Attributes
    ----------
    start : pd.Timestamp
        The start of the period.
    end : pd.Timestamp
        The end of the period.
    """

    start: Annotated[pd.Timestamp, PlainValidator(lambda x: pd.Timestamp(x))]
    end: Annotated[pd.Timestamp, PlainValidator(lambda x: pd.Timestamp(x))]


class Coding(TypedDict):
    """
    A TypedDict representing a FHIR Coding.

    Attributes
    ----------
    code : str
        The code of the coding.
    system : str
        The system of the coding.
    """

    code: str
    system: str


class CodeableConcept(TypedDict):
    """
    A TypedDict representing a FHIR CodeableConcept.

    Attributes
    ----------
    coding : Coding
        The coding of the CodeableConcept.
    """

    coding: Coding


class CodeableReference(TypedDict):
    """
    A TypedDict representing a FHIR CodeableReference.

    Attributes
    ----------
    concept : CodeableConcept
        The CodeableConcept of the CodeableReference.
    """

    concept: CodeableConcept


class AbstractFHIRSinkSchema(AbstractSinkSchema):
    """
    Abstract class for FHIR sink schemas.

    This class is used to define the structure of a FHIR sink schema. It is an abstract class
    that should be inherited by the specific FHIR sink schemas.
    """

    pass
