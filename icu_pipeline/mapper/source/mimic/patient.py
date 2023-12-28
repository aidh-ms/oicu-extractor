from icu_pipeline.mapper.source.mimic import AbstractMimicEventsMapper


class GenderMapper(AbstractMimicEventsMapper):
    SQL_QUERY = """
        SELECT *, anchor_year as charttime, gender as valueuom,
            CASE
                WHEN gender = 'F' THEN 0
                WHEN gender = 'M' THEN 1
            END valuenum
        FROM mimiciv_hosp.patients;
    """


class AgeMapper(AbstractMimicEventsMapper):
    SQL_QUERY = "SELECT *, anchor_year as charttime, anchor_age as valuenum, 'years' as valueuom FROM mimiciv_hosp.patients;"  # fmt: skip
