# 🧠 PRD: Google Analytics Extractor

## tl;dr

Daily ETL pipeline that extracts analytics data from Google Analytics v3 and v4 APIs, handles pagination and error recovery, and loads it into a data warehouse — enabling daily insights for marketing and product decisions.

---

## 🎯 Goals

- **Automated Daily Extraction**: Pull analytics data from GA v3 and v4 on a scheduled basis without manual intervention
- **API Version Compatibility**: Support both Google Analytics v3 (Universal Analytics) and v4 (GA4) with version-specific query strategies
- **Pagination Management**: Handle large result sets through date splitting, data segmentation, and block-based fetching
- **Error Resilience**: Implement retries and error handling to prevent data gaps
- **Warehouse Loading**: Deliver clean, structured data to the data warehouse for downstream analysis

## 👤 User Stories

- As a **marketing analyst**, I want daily GA data in the warehouse so I can build dashboards and make timely decisions
- As a **product manager**, I want both v3 and v4 data available so I can compare historical and current metrics
- As a **data engineer**, I want the pipeline to handle pagination and API limits gracefully so extraction runs complete reliably
- As a **data engineer**, I want error recovery so transient API failures don't create data gaps

## 🔄 Extraction Flow

```text
1. Scheduler triggers daily extraction job
   ↓
2. Determine API version and parameters for each query
   ↓
3. Submit query to Google Analytics API (v3 or v4)
   ↓
4. Handle pagination: split by date ranges, segment into blocks
   ↓
5. Retry on transient errors with exponential backoff
   ↓
6. Aggregate and transform extracted data
   ↓
7. Load structured data into the data warehouse
```

## 🧱 Core Components

### Extraction Engine

- **GA v3 Client**: Queries Universal Analytics API with v3-specific parameters and pagination
- **GA v4 Client**: Queries GA4 API with v4-specific parameters and date-range handling
- **Query Builder**: Constructs API requests based on metrics, dimensions, and date ranges
- **Pagination Handler**: Splits queries by calendar dates, segments data into blocks, adjusts parameters for large result sets

### Error Handling

- **Retry Logic**: Exponential backoff for transient API errors (rate limits, server errors)
- **Error Logging**: Structured logging for failed queries with context for debugging
- **Gap Detection**: Identifies missing date ranges and re-queues failed extractions

### Data Pipeline

- **Transform Layer**: Normalizes v3 and v4 data into a unified schema
- **Load Layer**: Writes to data warehouse with idempotent upserts

## 📚 References

- [Architecture Diagram](./diagram.puml)