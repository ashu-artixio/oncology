# Oncology Automate

A modular ontology ingestion and normalization platform that fetches, processes, and stores oncology-related disease ontology data (based on MONDO) into a structured PostgreSQL database.

---

## 🧠 Overview

**Oncology Automate** is designed to automatically collect, normalize, and synchronize disease ontology data — particularly oncology-related diseases — from the **MONDO (Monarch Disease Ontology)** dataset.  
It standardizes hierarchical disease relationships and integrates them into a unified database schema for downstream analytics, regulatory systems, or clinical use.

The platform features a **modular architecture**, allowing flexible extensions for additional ontology sources and fully automated ingestion workflows.

---

## 🚀 Key Features

- 🔄 **Modular Architecture:** Each stage (fetch, normalize, map, ingest) runs as an independent module.  
- 🧬 **Ontology Normalization:** Standardizes MONDO ontology nodes, synonyms, and relationships.  
- 🗃️ **Database Integration:** Inserts clean, deduplicated records directly into PostgreSQL.  
- 🐳 **Docker Ready:** Fully containerized for easy deployment and scheduling.  
- ⚙️ **Configurable Pipelines:** Enable/disable modules via `config.json`.  
- 🧩 **Re-runnable:** Safe re-execution with duplicate prevention.  
- 📝 **Detailed Logging:** Full activity and statistics logging for each run.  
- 🔁 **Retry Logic:** Automatic retry with exponential backoff on transient failures.  
- 🧪 **Trial Mode:** Process limited records for testing before full ingestion.  

---

## 📁 Project Structure

```
Oncology/
├── app.py                   # Main orchestrator for the ingestion pipeline
├── config.json              # Module enable/disable and global settings
├── docker-compose.yml       # Docker orchestration for deployment
├── Dockerfile               # Container image definition
├── mondo_fetcher.py         # Fetches MONDO ontology dataset (JSON/OWL)
├── mondo_normalizer.py      # Normalizes MONDO ontology structure and metadata
├── mondo_db_mapper.py       # Maps normalized ontology into database tables
├── mondo_ingest_runner.py   # Coordinates ingestion workflow end-to-end
├── requirements.txt          # Python dependencies
└── __pycache__/              # Compiled bytecode (auto-generated)
```

---

## 🧩 Modules

### 1. MONDO Fetcher Module ✅ (Active)
**Source:** [MONDO Disease Ontology](https://github.com/monarch-initiative/mondo)

Fetches and parses the latest MONDO dataset (in JSON or OWL format).

**Features:**
- Downloads full MONDO ontology file.  
- Extracts nodes, labels, synonyms, and parent relationships.  
- Supports incremental updates (if available).  
- Logs total nodes, relationships, and metadata count.  

---

### 2. MONDO Normalizer Module ✅ (Active)

Cleans and standardizes fetched ontology data.

**Features:**
- Resolves duplicate synonyms and overlapping IDs.  
- Normalizes field names and ontology hierarchy.  
- Filters oncology-related diseases (if enabled).  
- Outputs a flattened JSON structure.  

---

### 3. Database Mapper Module ✅ (Active)

Maps normalized MONDO records to your PostgreSQL schema.

**Features:**
- Converts hierarchical ontology into table-compatible rows.  
- Prevents duplicate insertions using unique ontology IDs.  
- Supports trial mode for limited inserts.  
- Logs record counts before and after insertion.  

---

### 4. Ingestion Runner (Main Orchestrator)

Runs the complete fetch → normalize → map → ingest pipeline sequentially or selectively.

---

## ⚙️ Configuration

### Global Configuration (`config.json`)

Controls which modules run and how errors are handled.

```json
{
  "modules": {
    "mondo_fetcher": { "enabled": true, "description": "Fetch MONDO ontology data" },
    "mondo_normalizer": { "enabled": true, "description": "Normalize MONDO data" },
    "mondo_db_mapper": { "enabled": true, "description": "Map and insert data into PostgreSQL" }
  },
  "settings": {
    "stop_on_error": false,
    "trial_limit": 0,
    "log_level": "INFO"
  }
}
```

### Environment Variables (`.env`)

```
PG_HOST=localhost
PG_PORT=5432
PG_DATABASE=ontology_db
PG_USER=postgres
PG_PASSWORD=your_password
LOG_LEVEL=INFO
```

---

## ⚡ Quick Start

### Option 1: Local Setup

#### 1. Clone and Navigate
```bash
git clone <repository-url>
cd Oncology
```

#### 2. Setup Environment
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

#### 3. Configure Database Credentials
Create a `.env` file as shown above.

#### 4. Run Pipeline
```bash
# List available modules
python app.py --list

# Run all enabled modules
python app.py all

# Run a specific module
python app.py mondo_fetcher
```

---

### Option 2: Docker Setup

#### 1. Build and Run
```bash
docker-compose up --build
```

#### 2. Background Mode
```bash
docker-compose up -d
```

#### 3. View Logs
```bash
docker-compose logs -f
```

---

## 🧪 Trial Mode (Testing)

Edit `config.json` and set:
```json
"trial_limit": 100
```

Then run:
```bash
python app.py mondo_ingest_runner
```

---

## 🗃️ Database Schema

**Target Table:** `ontology.mondo_ontology_records`

| Column Name         | Source Field         | Description |
|----------------------|---------------------|--------------|
| mondo_id             | `id`                | MONDO unique ID |
| disease_name         | `label`             | Disease or condition name |
| synonyms             | `synonyms`          | List of alternate names |
| parent_id            | `is_a`              | Parent ontology term |
| definition           | `definition`        | Formal MONDO definition |
| ontology_type        | `subset`            | Subcategory/type (e.g., neoplastic, rare) |
| source_json          | Full MONDO object   | Raw source JSON for reference |
| created_at           | Generated timestamp | Record insertion time |

Duplicate prevention is ensured using the `mondo_id` key.

---

## 📊 Logging & Monitoring

**Local Logs:**  
`Oncology/logs/mondo_ingest.log`

**Docker Logs:**  
```bash
docker-compose logs -f
```

**Log Levels:**  
Configured via `.env`:
```
LOG_LEVEL=DEBUG | INFO | WARNING | ERROR | CRITICAL
```

---

## 🕒 Scheduling (Cron)

Automate daily ingestion runs:

```bash
crontab -e
# Run daily at 3 AM
0 3 * * * cd /path/to/Oncology && docker-compose up >> /var/log/oncology-automate.log 2>&1
```

---

## 🧱 Development

### Adding a New Module

1. Create a new folder:  
   `Oncology/new_source/`
2. Implement a `main()` entry function.
3. Add it to `config.json` under `"modules"`.
4. Document it with a `README.md`.

---

## 🧰 Dependencies

See `requirements.txt` for full list.

**Key Libraries:**
- `pandas` – Data transformation  
- `psycopg2-binary` – PostgreSQL adapter  
- `requests` – Data fetching  
- `sqlalchemy` – ORM integration  
- `pydantic` – Data validation  
- `tenacity` – Retry logic  

---

## 🧩 Roadmap

- ✅ MONDO ingestion and normalization pipeline  
- ✅ PostgreSQL integration with duplicate prevention  
- ✅ Docker orchestration  
- 🚧 Integration with other ontology sources (EFO, DOID)  
- 🚧 Web API for ontology search and relationships  
- 🚧 Incremental (delta) updates  
- 🚧 Validation dashboards and data lineage reports  

---

## 👥 Authors

Ashutosh Sultania

---

## 🙏 Acknowledgments

- **MONDO Ontology Project** — for open access to disease ontology data  
- **PostgreSQL** — robust open-source database backend  
- **Python & Open Source Community** — foundational libraries enabling this system  

---

### 🧾 Version: 1.0.0  
**Last Updated:** November 14, 2025  
