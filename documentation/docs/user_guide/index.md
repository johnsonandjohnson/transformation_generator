# User Guide

The Transformation Generator can be configured by the user through the following input types:

## Project Config
This is the main input object driving the configuration of the Transformation Generator. The `Project Config` contains list of [Config Entry](./input_types/config_entry.md) objects and information about each `Config Entry` such as scheduling details.

## Config Entry
This input describes the target table being created. Each `Config Entry` will contain information about the target table such as its `target_type` ('table', 'view', 'program', or 'lineage) any explicit dependencies and the input files its being created from: the [Data Mapping](./input_types/data_mapping.md)

## Data Mapping
The `Data Mapping` input object contains dependency information about the target table.

## Table Definition
The `Table Definition` input object defines the target table schema with one `Table Definition` required for each target table defined by the [Config Entry](./input_types/config-entry.md)

## Running the Transformation Generator

The Transformation Generator provides the following execution scripts:

### [generate_data_linage.py](./generators/generate_data_linage.md)
general description.

### [generate_ddl_output.py](./generators/generate_ddl_output.md)
general description.

### [generate_orchestration.py](./generators/generate_orchestration.md)
general description.

### [generate_sql_output.py](./generators/generate_sql_output.md)
general description.