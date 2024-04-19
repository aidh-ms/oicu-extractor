from abc import ABCMeta
from typing import Type

from icu_pipeline.concepts import AbstractConcept
from icu_pipeline.mapper.source import AbstractSourceMapper, DataSource


class AbstractMetaConcept(AbstractConcept, metaclass=ABCMeta):
    CONCEPT_TYPE = "meta"
