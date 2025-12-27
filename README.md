# MongoDB to PostgreSQL Migration - DBT Project

Complete guide for migrating a multi-facility diabetes management system from MongoDB to PostgreSQL using DBT (Data Build Tool).

---

## 📋 Table of Contents

- [Overview](#overview)
- [Migration Strategy](#migration-strategy)
- [Prerequisites](#prerequisites)
- [Project Setup](#project-setup)
- [MongoDB Export](#mongodb-export)
- [DBT Project Structure](#dbt-project-structure)
- [Running the Migration](#running-the-migration)
- [Data Verification](#data-verification)
- [Common Commands](#common-commands)
- [Troubleshooting](#troubleshooting)
- [Post-Migration](#post-migration)

---

## 🎯 Overview

This project migrates a healthcare database from MongoDB to PostgreSQL with the following goals:

- **Data Integrity**: Preserve all patient records, user data, and medical history
- **Schema Normalization**: Transform NoSQL documents into normalized relational tables
- **Temporal Tracking**: Implement proper versioning for patient medical records
- **Data Quality**: Validate and test all migrated data
- **Auditability**: Maintain complete migration audit trail

### What's Being Migrated

| MongoDB Collection | PostgreSQL Table(s) | Row Count |
|-------------------|---------------------|-----------|
| `users` | `marts.users`, `marts.user_facilities` | 47 |
| `roles` | `marts.roles` (mapped) | - |
| `patients` | `marts.patients` | ~thousands |
| `patient_history` | `marts.patient_versions` | 13,713 |
| `patient_followups` | `marts.patient_versions` | 3,119 |
| `organizations` | `marts.facilities` | 5+ |

### Migration Approach

```
MongoDB Collections (CSV Export)
    ↓
DBT Seeds (Load into PostgreSQL)
    ↓
DBT Staging Models (Clean & Type)
    ↓
DBT Intermediate Models (Transform & Map)
    ↓
DBT Marts (Final Production Tables)
```

---

## ✅ Prerequisites

### Required Software

- **Python**: 3.11 or higher
- **PostgreSQL**: 16 or higher
- **Git**: Latest version
- **uv**: Python package installer

### Database Access

- PostgreSQL database (Neon, RDS, or local)
- Connection credentials (host, user, password, database name)
- Sufficient permissions to create schemas and tables

---

## 🚀 Project Setup

### Step 1: Install uv

```bash
# macOS/Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# Windows
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"

# Verify installation
uv --version
```

### Step 2: Create New DBT Project

```bash
# Create project directory
mkdir platform-db-migrate
cd platform-db-migrate

# Initialize uv project
uv init

# Activate virtual environment
source .venv/bin/activate

# Install DBT Core and PostgreSQL adapter
uv add dbt-postgres

# Verify installation
dbt --version
```

### Step 3: Initialize DBT Project

```bash
# Create DBT project
dbt init dbt_project

# Navigate to project
cd dbt_project

# Install dependencies (creates packages)
dbt deps
```

### Step 4: Configure Database Connection

#### Create .env file
cat > .env << 'EOF'
DB_HOST=your-database-host.neon.tech
DB_USER=your-username
DB_PASSWORD=your-password
DB_NAME=your-database
EOF

#### Load environment variables
export $(cat .env | xargs)

#### Test connection
dbt debug

---

## 📦 MongoDB Export

### Step 1: Export Collections to CSV

1. Install MongoDB Compass
2. Export as CSV


### Step 2: Create Mapping CSV

### Step 3: Create Lookup Tables

---

## 📁 DBT Project Structure

```
dbt_project/
├── dbt_project.yml
├── profiles.yml
├── packages.yml
├── macros/
│   ├── generate_version_number.sql
│   └── calculate_bmi.sql
├── models/
│   ├── staging/
│   │   ├── schema.yml
│   │   ├── stg_users.sql
│   │   ├── stg_roles.sql
│   │   ├── stg_patients.sql
│   │   ├── stg_patient_history.sql
│   │   └── stg_patient_followups.sql
│   ├── intermediate/
│   │   ├── schema.yml
│   │   ├── int_facilities.sql
│   │   ├── int_users_with_roles.sql
│   │   ├── int_user_facilities.sql
│   │   └── int_patient_versions.sql
│   └── marts/
│       ├── core/
│       │   ├── schema.yml
│       │   ├── facilities.sql
│       │   ├── users.sql
│       │   └── user_facilities.sql
│       ├── patients/
│       │   ├── schema.yml
│       │   ├── patients.sql
│       │   ├── patient_versions.sql
│       │   └── patient_transfers.sql
│       └── lookups/
│           ├── schema.yml
│           ├── diagnosis_types.sql
│           ├── complication_types.sql
│           └── medications.sql
├── seeds/
│   ├── facility_mapping.csv
│   ├── lookups/
│   │   ├── diagnosis_types.csv
│   │   ├── complication_types.csv
│   │   ├── education_levels.csv
│   │   └── medications.csv
│   └── mongodb/
│       └── medicaldb/
│           ├── users.csv
│           ├── patients.csv
│           ├── patient_history.csv
│           └── patient_followups.csv
└── tests/
    └── data_quality/
        └── assert_no_duplicate_patient_codes.sql
```

---

## ▶️ Running the Migration

### Phase 1: Load Seeds (MongoDB Exports)

```bash
# Load all seed files into PostgreSQL
dbt seed

# Or load specific seeds
dbt seed --select tag:lookups
dbt seed --select medicaldb.users
dbt seed --select facility_mapping

# Full refresh if data changed
dbt seed --full-refresh

# Verify seed loaded
dbt show --select medicaldb.users
```

### Phase 2: Run Staging Models

```bash
# Create staging views (clean MongoDB data)
dbt run --select staging

# Verify staging models
dbt show --select stg_users
dbt show --select stg_patients --limit 10
```

### Phase 3: Run Intermediate Models

```bash
# Run intermediate transformations
dbt run --select intermediate

# Verify intermediate models
dbt show --select int_facilities
dbt show --select int_users_with_roles
```

### Phase 4: Run Marts (Final Tables)

```bash
# Create production tables
dbt run --select marts

# Or run specific marts
dbt run --select marts.core
dbt run --select marts.patients
dbt run --select marts.lookups
```

### Phase 5: Run Tests

```bash
# Run all data quality tests
dbt test

# Run tests for specific models
dbt test --select patients
dbt test --select patient_versions

# Store test failures for review
dbt test --store-failures
```

### Complete Migration (All Phases)

```bash
# Run everything in order
dbt seed && dbt run && dbt test

# Or use build (recommended)
dbt build
```

---

## ✅ Data Verification

### Check Row Counts

```bash
# Compare MongoDB vs PostgreSQL counts
# MongoDB
mongo diabetes_db --eval "db.users.count()"
mongo diabetes_db --eval "db.patients.count()"
mongo diabetes_db --eval "db.patient_history.count()"

# PostgreSQL
psql $DATABASE_URL << 'EOF'
SELECT 'users' as table_name, COUNT(*) FROM marts.users
UNION ALL
SELECT 'patients', COUNT(*) FROM marts.patients
UNION ALL
SELECT 'patient_versions', COUNT(*) FROM marts.patient_versions;
EOF
```

### Verify Data Quality

```bash
# Check for duplicate patient codes
psql $DATABASE_URL << 'EOF'
SELECT code, COUNT(*) 
FROM marts.patients 
GROUP BY code 
HAVING COUNT(*) > 1;
EOF

# Check for patients without facilities
psql $DATABASE_URL << 'EOF'
SELECT COUNT(*) 
FROM marts.patients 
WHERE current_facility_id IS NULL;
EOF

# Check for orphaned records
psql $DATABASE_URL << 'EOF'
SELECT COUNT(*) 
FROM marts.patient_versions pv
LEFT JOIN marts.patients p ON pv.patient_id = p.id
WHERE p.id IS NULL;
EOF

# Check temporal integrity
psql $DATABASE_URL << 'EOF'
SELECT 
    patient_id,
    COUNT(*) as version_count,
    SUM(CASE WHEN is_current THEN 1 ELSE 0 END) as current_count
FROM marts.patient_versions
GROUP BY patient_id
HAVING SUM(CASE WHEN is_current THEN 1 ELSE 0 END) != 1;
EOF
```

### Sample Data Comparison

```bash
# MongoDB sample
mongo diabetes_db --eval "db.patients.findOne()"

# PostgreSQL sample
psql $DATABASE_URL -c "
SELECT * FROM marts.patients LIMIT 1;
SELECT * FROM marts.patient_versions WHERE patient_id = '<patient-id>' ORDER BY version_number;
"
```

---

## 📝 Common Commands

### DBT Operations

```bash
# Clean compiled files
dbt clean

# Compile without running
dbt compile

# Parse project
dbt parse

# List all models
dbt list

# List models by selector
dbt list --select tag:marts
dbt list --select +patients

# Show compiled SQL
dbt show --select patients --limit 5

# Run with logging
dbt run --log-level debug

# Generate documentation
dbt docs generate

# Serve documentation
dbt docs serve --port 8080
```

### Model-Specific Commands

```bash
# Run model and all upstream dependencies
dbt run --select +patients

# Run model and all downstream dependencies
dbt run --select patients+

# Run model and ALL dependencies
dbt run --select +patients+

# Run modified models only (for CI)
dbt run --select state:modified+

# Run by tag
dbt run --select tag:pii
dbt run --select tag:lookups

# Exclude models
dbt run --exclude tag:temporary
```

### Testing Commands

```bash
# Test specific model
dbt test --select patients

# Test relationships
dbt test --select test_type:relationships

# Test unique/not_null
dbt test --select test_type:schema

# Custom data tests
dbt test --select test_type:data

# Show test failures
dbt test --store-failures
psql $DATABASE_URL -c "SELECT * FROM dbt_test__audit.unique_patients_code;"
```

### Seed Commands

```bash
# Load all seeds
dbt seed

# Load specific seed
dbt seed --select facility_mapping

# Full refresh (drop and reload)
dbt seed --full-refresh

# Show seed data
dbt show --select facility_mapping
```

---

## 🐛 Troubleshooting

### Common Issues

#### 1. Connection Errors

```bash
# Test database connection
dbt debug

# Check environment variables
echo $DB_HOST
echo $DB_USER
echo $DB_NAME

# Test direct PostgreSQL connection
psql "postgresql://$DB_USER:$DB_PASSWORD@$DB_HOST:5432/$DB_NAME"
```

#### 2. Seed Loading Errors

```bash
# Column type mismatch
# Solution: Update dbt_project.yml with correct column types
seeds:
  dbt_project:
    mongodb:
      medicaldb:
        patient_latest_data:
          +column_types:
            contactNumber: bigint
            contactNumber2: bigint

# Array columns in CSV
# Solution: Use bracket notation in CSV: field_0,field_1,field_2
# Then combine in staging model:
# ARRAY_REMOVE(ARRAY[field_0, field_1, field_2], NULL)
```

#### 3. Compilation Errors

```bash
# Clear cache and recompile
dbt clean
dbt deps
dbt compile

# Check for circular dependencies
dbt list --select +model_name

# Validate SQL syntax
dbt compile --select model_name
```

#### 4. UUID vs Text Issues

```bash
# Error: invalid input syntax for type uuid: "1"
# Solution: Don't use COALESCE with UUIDs and text
# WRONG: COALESCE(uuid_field, '1')::text
# RIGHT: uuid_field (can be NULL)
# OR: COALESCE(uuid_field, (SELECT id FROM ref_table LIMIT 1))
```

#### 5. Column Not Found Errors

```bash
# Error: column "column_name" does not exist
# Solution: Check staging model for actual column names
dbt compile --select staging_model_name
cat target/compiled/dbt_project/models/staging/stg_model.sql

# Add NULL placeholders for missing columns
NULL::text AS missing_column
```

#### 6. Test Failures

```bash
# View which tests failed
dbt test --store-failures

# Query failed tests
psql $DATABASE_URL -c "
SELECT * FROM dbt_test__audit.not_null_patients_id;
"

# Fix data issues and re-run
dbt run --select patients --full-refresh
dbt test --select patients
```

### Performance Issues

```bash
# Increase threads
dbt run --threads 8

# Run specific models
dbt run --select marts.core

# Use materialized tables for large datasets
# In model config:
{{ config(materialized='table') }}

# Add indexes via post-hooks
{{
    config(
        post_hook=[
            "CREATE INDEX IF NOT EXISTS idx_patients_code ON {{ this }} (code)"
        ]
    )
}}
```

### Debugging Tips

```bash
# Enable verbose logging
dbt run --log-level debug --select model_name

# View compiled SQL
dbt compile --select model_name
cat target/compiled/dbt_project/models/path/to/model.sql

# Run compiled SQL directly
dbt compile --select model_name
psql $DATABASE_URL < target/run/dbt_project/models/path/to/model.sql

# Check DBT logs
cat logs/dbt.log
tail -f logs/dbt.log

# Profile performance
dbt run --select model_name --profile
```

---

## 🎯 Post-Migration

### Cleanup

```bash
# Drop temporary seeds after successful migration
psql $DATABASE_URL << 'EOF'
DROP TABLE IF EXISTS seeds.medicaldb_users CASCADE;
DROP TABLE IF EXISTS seeds.medicaldb_patients CASCADE;
DROP TABLE IF EXISTS seeds.medicaldb_patient_history CASCADE;
DROP TABLE IF EXISTS seeds.medicaldb_patient_followups CASCADE;
DROP TABLE IF EXISTS seeds.facility_mapping CASCADE;
EOF

# Optional: Drop entire seeds schema
psql $DATABASE_URL -c "DROP SCHEMA IF EXISTS seeds CASCADE;"

# Drop staging schema (views)
psql $DATABASE_URL -c "DROP SCHEMA IF EXISTS staging CASCADE;"

# Keep marts schema (production data)
```

### Add Primary Keys (Optional)

```bash
# Add primary key constraints for better performance
psql $DATABASE_URL << 'EOF'
ALTER TABLE marts.facilities ADD PRIMARY KEY (id);
ALTER TABLE marts.users ADD PRIMARY KEY (id);
ALTER TABLE marts.patients ADD PRIMARY KEY (id);
ALTER TABLE marts.patient_versions ADD PRIMARY KEY (id);
ALTER TABLE marts.user_facilities ADD PRIMARY KEY (id);
EOF
```

### Add Foreign Keys (Optional)

```bash
# Add foreign key constraints
psql $DATABASE_URL << 'EOF'
ALTER TABLE marts.patients 
  ADD CONSTRAINT fk_patients_facility 
  FOREIGN KEY (current_facility_id) 
  REFERENCES marts.facilities(id);

ALTER TABLE marts.users 
  ADD CONSTRAINT fk_users_facility 
  FOREIGN KEY (primary_facility_id) 
  REFERENCES marts.facilities(id);

ALTER TABLE marts.patient_versions 
  ADD CONSTRAINT fk_versions_patient 
  FOREIGN KEY (patient_id) 
  REFERENCES marts.patients(id);
EOF
```

### Monitoring

```bash
# Check database size
psql $DATABASE_URL -c "
SELECT 
    pg_size_pretty(pg_database_size(current_database())) as database_size;
"

# Check table sizes
psql $DATABASE_URL -c "
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE schemaname = 'marts'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
"

# Check query performance
psql $DATABASE_URL -c "
SELECT 
    query,
    calls,
    mean_exec_time,
    max_exec_time
FROM pg_stat_statements
ORDER BY mean_exec_time DESC
LIMIT 10;
"
```

---

## 📚 Additional Resources

### DBT Resources
- [DBT Documentation](https://docs.getdbt.com)
- [DBT Best Practices](https://docs.getdbt.com/guides/best-practices)
- [DBT Discourse Community](https://discourse.getdbt.com)
- [DBT Slack Community](https://www.getdbt.com/community/join-the-community/)

### PostgreSQL Resources
- [PostgreSQL Documentation](https://www.postgresql.org/docs)
- [PostgreSQL Performance Tips](https://wiki.postgresql.org/wiki/Performance_Optimization)
- [PostGIS (if using geo data)](https://postgis.net)

### Migration Resources
- [MongoDB to PostgreSQL Migration Guide](https://www.mongodb.com/developer/products/mongodb/migrate-from-mongodb-to-postgresql/)
- [Data Migration Best Practices](https://martinfowler.com/articles/evodb.html)

---

## 🤝 Support

For issues or questions:
1. Check the [Troubleshooting](#troubleshooting) section
2. Review DBT logs: `logs/dbt.log`
3. Create an issue in the repository
4. Contact the data team

---

## 📄 License

This project is licensed under the MIT License.

---

**Migration completed successfully!** 🎉

Your MongoDB data is now in PostgreSQL with:
- ✅ Proper relational schema
- ✅ Temporal patient history
- ✅ Data quality tests
- ✅ Full audit trail
- ✅ Documentation