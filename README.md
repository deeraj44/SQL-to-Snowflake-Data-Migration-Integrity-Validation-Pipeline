# Project Overview

This project simulates an enterprise-grade data migration from a legacy PostgreSQL system to Snowflake. Using a financial transaction dataset (284K+ records), I designed a normalized relational schema, implemented a chunked ETL pipeline, and built an automated reconciliation framework to validate data integrity post-migration.

The objective was to replicate a real-world SQL → Snowflake transition scenario where financial accuracy, referential integrity, and audit validation are critical.

# Architecture
Kaggle CSV  
    ↓  
PostgreSQL (Source System)  
    ↓  
Python Migration Pipeline (Chunked ETL)  
    ↓  
Snowflake (Target System)  
    ↓  
Automated Reconciliation Report

# Dataset
Source: Kaggle Credit Card Fraud Detection Dataset (2023)

568,630 financial transactions

Features: V1–V28 (anonymized attributes)

Amount: Transaction value

Class: Fraud label (0/1)

The flat CSV was normalized into a relational schema to simulate enterprise database migration.

# Schema Design

To mirror real financial systems, the dataset was split into three relational tables:

### 1️⃣ transactions  
txn_id (Primary Key)  
amount  
ingested_at  

### 2️⃣ transaction_features  
txn_id (Foreign Key)  
v1–v28 (anonymized attributes)  

### 3️⃣ fraud_labels  
txn_id (Foreign Key)  
is_fraud (0/1)  
This design demonstrates normalization, constraint management, and referential integrity.

### 🔄 Migration Process  
#### Step 1: Source Profiling    
Row counts    
Duplicate ID detection    
Fraud rate calculation    
Min/Max/Average transaction amounts    

#### Step 2: PostgreSQL Source Setup  
Created relational schema  
Applied primary/foreign keys  
Indexed transaction ID  
Loaded dataset in controlled order  

#### Step 3: Snowflake Target Setup  
Created database and schema  
Mapped data types (Postgres → Snowflake)  
Recreated normalized tables  

#### Step 4: Migration Execution  
Extracted data in 50K row chunks  
Inserted into Snowflake using Python connector  
Implemented rerunnable migration (TRUNCATE before reload)  

# Data Integrity Validation

A reconciliation framework was implemented to ensure accurate migration.   
Validation checks included:  
Row count comparison (source vs target)  
Fraud totals validation  
Min/Max transaction amount comparison  
Aggregate metric verification  
PASS/FAIL reporting output  
All checks returned matching results, confirming successful data migration.  

# Technologies Used

- PostgreSQL
- Snowflake
- Python (pandas, SQLAlchemy, Snowflake Connector)
- SQL

# Key Outcomes

Migrated 568K+ financial transaction records across 3 relational tables  
Designed a normalized schema to simulate enterprise financial systems  
Built automated reconciliation checks ensuring 100% data integrity  
Developed a rerunnable, chunked ETL pipeline for scalable migration  

# What This Demonstrates

This project showcases practical experience in:

Data migration between heterogeneous systems

Schema design and normalization

ETL pipeline development

Financial data validation and reconciliation

Cross-system data integrity assurance

