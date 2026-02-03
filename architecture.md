# Architecture Documentation

## System Architecture

### Overview
Multi-source sales analytics pipeline built on Azure Databricks using Delta Lake and Medallion Architecture pattern.
```
┌─────────────────────────────────────────────────────────────┐
│                    DATA SOURCES (3)                          │
├─────────────────────────────────────────────────────────────┤
│  Legacy POS      E-commerce        Mobile App               │
│  (CSV)           (JSON)            (Parquet)                 │
│  4,200 txns      3,675 txns        2,625 txns               │
└──────┬────────────────┬─────────────────┬───────────────────┘
       │                │                 │
       ▼                ▼                 ▼
┌─────────────────────────────────────────────────────────────┐
│              BRONZE LAYER (Raw Data)                         │
│              Azure ADLS Gen2 (abfss://)                      │
├─────────────────────────────────────────────────────────────┤
│  bronze.pos_raw                                              │
│  bronze.ecommerce_raw                                        │
│  bronze.mobile_raw                                           │
│  ✓ Delta Lake format                                         │
│  ✓ Time travel enabled                                       │
│  ✓ Schema evolution                                          │
└──────┬──────────────────────────────────────────────────────┘
       │
       │  Transformations:
       │  • Date parsing (6+ formats)
       │  • Price cleaning
       │  • Schema harmonization
       │  • Quality scoring
       │
       ▼
┌─────────────────────────────────────────────────────────────┐
│           SILVER LAYER (Cleaned & Unified)                   │
├─────────────────────────────────────────────────────────────┤
│  silver.unified_sales (10,000+ records)                      │
│  silver.unified_sales_deduped                                │
│  silver.sales_rejected                                       │
│  ✓ Unified schema                                            │
│  ✓ Quality tiers (high/medium/low/rejected)                 │
│  ✓ Duplicate detection                                       │
└──────┬──────────────────────────────────────────────────────┘
       │
       │  Aggregations:
       │  • Revenue metrics
       │  • Product analytics
       │  • Quality dashboards
       │
       ▼
┌─────────────────────────────────────────────────────────────┐
│            GOLD LAYER (Business Metrics)                     │
├─────────────────────────────────────────────────────────────┤
│  gold.revenue_by_source                                      │
│  gold.product_performance                                    │
│  gold.data_quality_metrics                                   │
│  gold.executive_summary                                      │
│  ✓ Optimized for BI tools                                    │
│  ✓ Pre-aggregated                                            │
└──────┬──────────────────────────────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────────────────────────────┐
│              VISUALIZATION LAYER                             │
├─────────────────────────────────────────────────────────────┤
│  • Databricks SQL Dashboards                                 │
│  • Power BI Reports                                          │
│  • Executive KPIs                                            │
└─────────────────────────────────────────────────────────────┘
```

## Data Flow

### 1. Ingestion (Bronze)
- **Source 1:** Legacy POS exports CSV files daily
- **Source 2:** E-commerce platform provides JSON via API
- **Source 3:** Mobile app backend logs to Parquet

All data lands in Azure ADLS Gen2 as Delta tables.

### 2. Transformation (Silver)
**Quality Scoring Dimensions:**
1. Transaction ID completeness (-100 if missing)
2. Product ID completeness (-100 if missing)
3. Customer ID completeness (-10 if missing)
4. Date validity (-20 if invalid)
5. Price validity (-50 if invalid)
6. Quantity validity (-30 if invalid)
7. Status standardization (-5 if unknown)
8. Store ID presence (-5 if missing)

**Duplicate Detection:**
- Group by: customer + product + date
- Resolution: E-commerce > Mobile > POS priority

### 3. Analytics (Gold)
Pre-aggregated tables optimized for dashboards:
- Daily revenue by channel
- Product performance metrics
- Data quality trends

## Delta Lake Features Used

1. **ACID Transactions** - Ensures consistency
2. **Time Travel** - Query historical versions
3. **OPTIMIZE** - Compacts small files
4. **Z-ORDER** - Co-locates related data
5. **Schema Evolution** - Add columns dynamically

## Performance Characteristics

- **Ingestion:** 10K records in 5-10 seconds
- **Transformation:** Full pipeline in 20-30 seconds
- **Query Performance:** Sub-second for aggregated metrics
- **Storage:** ~50MB for 10K records (Delta compressed)

## Scalability

Current: 10K records/day  
Tested: Up to 100K records/day  
Max capacity: 1M+ records/day (with cluster scaling)
