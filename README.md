# Multisource-Sales-Analytics
End-to-end data pipeline integrating 3 sales channels using Azure Databricks, Delta Lake, and medallion architecture.

multi-source-sales-analytics-azure/
├── README.md
├── notebooks/
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_transformation.py
│   ├── 03_gold_analytics.py
│   └── complete_pipeline.py
├── data_generators/
│   ├── generate_pos_data.py
│   ├── generate_ecommerce_data.py
│   └── generate_mobile_data.py
├── docs/
│   ├── architecture.md
│   ├── data_quality_framework.md
│   └── setup_guide.md
├── screenshots/
│   ├── bronze_layer.png
│   ├── silver_quality.png
│   ├── gold_dashboard.png
│   └── azure_storage.png
└── requirements.txt
