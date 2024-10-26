# OpenICU Conceptbase

The OpenICU concept base works as a single source of truth for the concepts used in the OpenICU project.

## Tools

- [Athena: Search for codes here](https://athena.ohdsi.org/search-terms/start)
- [SNOMED Browser](https://browser.ihtsdotools.org)

## Data Sources

- eICU
  - [eICU Documentation](https://eicu-crd.mit.edu/about/eicu/): Find general schema and tables here
  - [eICU SchemaSpy](https://mit-lcp.github.io/eicu-schema-spy/)
- MIMIC-IV
  - [MIMIC Documentation](https://mimic.mit.edu/docs/)
  - Notes:
    - in mimic, laboratory values should be drawn from `mimiciv_hosp.labevents`

## Concepts

### Naming Conventions

- we're using pascal case for naming concepts, e.g. `HeartRate`
- specifications for concepts should be postfixed with an underscore, e.g. `WBC_Relative` and `WBC_Absolute`
- data sources should be postfixed at the end, e.g. `WBC_Relative_Serum`
  - Whole Blood: WB
