# Setup Guide

## Prerequisites

1. **Azure Account**
   - Active Azure subscription
   - Resource group created

2. **Azure Databricks**
   - Workspace created
   - Premium tier (for Unity Catalog features)

3. **Azure Storage**
   - Storage account with ADLS Gen2 enabled
   - Hierarchical namespace: **ENABLED**
   - Container created: `delta-tables`

4. **Permissions**
   - Storage account access key
   - Databricks workspace contributor access

---

## Step 1: Create Azure Resources

### 1.1 Create Storage Account

**Azure Portal:**
1. **Create a resource** → **Storage account**
2. **Resource group:** Your RG
3. **Storage account name:** `yourstorageaccount`
4. **Region:** Same as Databricks
5. **Performance:** Standard
6. **Redundancy:** LRS
7. **Advanced tab:**
   - ✅ **Enable hierarchical namespace** (CRITICAL!)
8. **Create**

### 1.2 Create Container

1. Navigate to storage account
2. **Containers** → **+ Container**
3. **Name:** `delta-tables`
4. **Public access level:** Private
5. **Create**

### 1.3 Get Access Key

1. Storage account → **Access keys**
2. **Show keys**
3. Copy **Key 1** (88 characters)

---

## Step 2: Set Up Databricks

### 2.1 Create Compute Cluster

**Databricks Workspace:**
1. **Compute** (left sidebar)
2. **Create compute**
3. **Configuration:**
   - **Cluster name:** `analytics-cluster`
   - **Cluster mode:** Single Node
   - **Databricks runtime:** 14.3 LTS
   - **Node type:** Standard_DS3_v2
   - **Auto-terminate:** 60 minutes
4. **Create**

### 2.2 Configure Storage Access

**In Notebook Cell 1:**
```python
STORAGE_ACCOUNT_NAME = "yourstorageaccount"
STORAGE_ACCOUNT_KEY = "YOUR_88_CHAR_KEY_HERE"
CONTAINER_NAME = "delta-tables"

spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net",
    STORAGE_ACCOUNT_KEY
)

BASE_PATH = f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"
```

---

## Step 3: Import Notebooks

### 3.1 Upload to Databricks

**Method 1: Import from File**
1. **Workspace** → **Create** → **Import**
2. Select downloaded `.py` or `.ipynb` files
3. Choose import location
4. **Import**

**Method 2: Clone from GitHub**
1. **Workspace** → **Create** → **Repo**
2. **Git repository URL:** Your GitHub repo
3. **Clone**

### 3.2 Notebook Execution Order

**Run in sequence:**
1. `01_bronze_ingestion.py` - Creates Bronze tables
2. `02_silver_transformation.py` - Creates Silver tables
3. `03_gold_analytics.py` - Creates Gold tables

---

## Step 4: Verify Installation

**Run this verification cell:**
```python
# Verify all tables exist
print("BRONZE LAYER:")
print(f"  pos_raw: {spark.table('bronze.pos_raw').count():,}")
print(f"  ecommerce_raw: {spark.table('bronze.ecommerce_raw').count():,}")
print(f"  mobile_raw: {spark.table('bronze.mobile_raw').count():,}")

print("\nSILVER LAYER:")
print(f"  unified_sales: {spark.table('silver.unified_sales').count():,}")

print("\nGOLD LAYER:")
print(f"  revenue_by_source: {spark.table('gold.revenue_by_source').count():,}")

# Verify Azure storage
files = dbutils.fs.ls(f"{BASE_PATH}/gold/")
print(f"\n✓ Gold layer has {len(files)} tables in Azure Storage")
```

**Expected output:**
```
BRONZE LAYER:
  pos_raw: 4,200
  ecommerce_raw: 3,675
  mobile_raw: 2,625

SILVER LAYER:
  unified_sales: 10,000

GOLD LAYER:
  revenue_by_source: 273

✓ Gold layer has 4 tables in Azure Storage
```

---

## Troubleshooting

### Issue: "Hierarchical namespace not enabled"
**Solution:** Recreate storage account with this feature enabled (can't enable after creation)

### Issue: "Cannot access storage"
**Solution:** 
- Verify storage key is correct (88 characters)
- Check storage account name matches
- Ensure container name is correct

### Issue: "Table not found"
**Solution:** Run notebooks in order (Bronze → Silver → Gold)

### Issue: "Cluster terminated"
**Solution:** Restart cluster (takes 3-5 minutes)

---

## Next Steps

1. ✅ Run all notebooks
2. ✅ Verify data in Azure Portal
3. ✅ Create Databricks SQL dashboards (Day 6)
4. ✅ Connect Power BI (Day 7)

---
