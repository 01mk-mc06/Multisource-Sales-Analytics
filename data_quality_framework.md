# Data Quality Framework

## Overview
Automated 8-dimension quality scoring system evaluating every record.

## Quality Dimensions

| Dimension | Weight | Missing | Invalid | Rationale |
|-----------|--------|---------|---------|-----------|
| Transaction ID | Critical | -100 | -100 | Can't track without ID |
| Product ID | Critical | -100 | -100 | Don't know what sold |
| Customer ID | High | -10 | N/A | Lose marketing insights |
| Date | High | -20 | N/A | Can't do time analysis |
| Price | High | -50 | -50 | Revenue calculations wrong |
| Quantity | Medium | -30 | -30 | Inventory tracking off |
| Status | Low | -5 | -5 | Can infer from context |
| Store ID | Low | -5 | N/A | Can aggregate without |

## Quality Tiers

**High (90-100)**
- Production-ready
- Use immediately in reports
- ~60-70% of records

**Medium (70-89)**
- Usable with caveats
- Schedule cleanup
- ~20-30% of records

**Low (50-69)**
- Don't use for critical decisions
- Investigate source issues
- ~5-10% of records

**Rejected (<50)**
- Quarantine immediately
- Fix source system
- ~5-10% of records

## Quality by Source

**POS (Legacy):** 67/100 average
- Issues: Multiple date formats, price symbols, missing customers
- Action: System upgrade planned

**E-commerce:** 85/100 average
- Issues: 10% missing customer emails
- Action: Form validation improvements

**Mobile:** 92/100 average
- Issues: Device IDs instead of customer names
- Action: Account linking feature

## Business Rules

### Rejection Criteria
- Missing transaction ID → Auto-reject
- Missing product ID → Auto-reject
- Invalid price (<= 0) → High penalty
- Both quantity and price missing → Reject

### Validation Logic
```python
# Date validation
if date not in valid_formats:
    quality_score -= 20

# Price validation
if price <= 0 or price is None:
    quality_score -= 50

# Cross-field validation
if quantity > 100 and unit_price < 1:
    flag_for_review()
```

## Monitoring

**Daily Quality Report:**
```sql
SELECT 
    source_system,
    quality_tier,
    COUNT(*) as records,
    AVG(quality_score) as avg_score
FROM silver.unified_sales
WHERE transformation_date = CURRENT_DATE
GROUP BY source_system, quality_tier
```

**Quality Trend (7 days):**
```sql
SELECT 
    DATE(transformation_timestamp) as date,
    AVG(quality_score) as avg_score
FROM silver.unified_sales
WHERE transformation_timestamp >= CURRENT_DATE - 7
GROUP BY DATE(transformation_timestamp)
ORDER BY date
```

## Improvement Actions

**Week 1-4:** Baseline measurement  
**Week 5-8:** Fix top 3 issues per source  
**Week 9-12:** Target 85% average quality  
**Ongoing:** Maintain 90%+ high-quality tier

## ROI

**Before Quality Scoring:**
- Unknown data reliability
- Manual spot checks (sample 100 records/week)
- Errors discovered in production reports

**After Quality Scoring:**
- 100% automated validation
- Real-time quality monitoring
- Proactive issue detection
- 40% reduction in data-related incidents
