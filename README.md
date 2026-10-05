# TPC-H Finance Lakehouse

Group Assignment 1 implementation for the Finance customer profile (Synevyr team).

## Goal

Build a Databricks Lakehouse pipeline using the TPC-H sample dataset:

samples.tpch

The solution implements:

- Bronze layer
- Silver layer
- Gold layer for Finance analytics
- Data quality validation
- Monitoring
- Finance business questions and visualisations

## Data source

TPC-H sample data provided by Databricks under:

samples.tpch

## Project structure

- `src/profiling` — source profiling
- `src/bronze` — Bronze ingestion
- `src/silver` — Silver transformations
- `src/validation` — data quality checks
- `src/gold` — Finance Gold tables
- `src/monitoring` — monitoring logic
- `notebooks` — Databricks exploration/demo notebooks
- `docs` — diagrams/documentation
- `presentation` — final presentation