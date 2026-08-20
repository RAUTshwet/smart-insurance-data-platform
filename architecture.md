# Smart Insurance Data Platform - Architecture

```text
Raw CSV
  |
  v
Bronze Layer
  |
  v
Data Quality Checks
  |
  v
Silver Layer
  |
  v
Business Transformations
  |
  v
Gold Layer
  |
  +--> SQL Analytics
  |
  +--> Power BI
```

## Layers

- **Raw:** source insurance CSV files.
- **Bronze:** ingested copies of raw data.
- **Silver:** cleaned, standardized and deduplicated data.
- **Gold:** business-ready datasets such as Customer 360, Policy Summary, Claims by Type and Payment Summary.
