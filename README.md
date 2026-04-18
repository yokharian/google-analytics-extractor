# Google Analytics Extractor

![Architecture](http://www.plantuml.com/plantuml/proxy?cache=no&src=https://raw.githubusercontent.com/yokharian/google-analytics-extractor/master/diagram.puml)

Daily ETL pipeline extracting data from Google Analytics v3 and v4 APIs into a data warehouse.

## Tech Stack

- **Language**: Python
- **APIs**: Google Analytics v3 (Universal Analytics), Google Analytics v4 (GA4)
- **Pattern**: ETL pipeline with scheduled extraction

## How It Works

1. Scheduler triggers daily extraction for configured metrics and dimensions
2. Queries are built with version-specific parameters (v3 or v4)
3. Large queries are split by date ranges and segmented into blocks
4. API responses are fetched with pagination handling
5. Transient errors trigger retries with exponential backoff
6. Data is normalized and loaded into the data warehouse

## Features

- Dual API support (GA v3 and v4)
- Smart pagination: date splitting, data segmentation, parameter adjustment
- Error recovery with retries and gap detection
- Idempotent warehouse loading
- Unified output schema across both API versions


## References

- [PRD](./prd.md)
- Blog post: [Google Analytics Data Extractor](https://yokharian.dev/posts/google-analytics-data-extractor)
