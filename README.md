# Email Extractor Bot

**Automated recruiter/vendor contact extraction from candidate Gmail inboxes, with LLM-powered job classification.**

This bot connects to candidate Gmail accounts over IMAP, reads incoming emails, filters out junk, extracts recruiter contact details (name, email, phone, company, LinkedIn), and saves them to the database through the Whitebox Learning API. It also classifies raw job listings using a local or cloud LLM.

---

## 🗂️ What This Bot Does

| Module | What it does |
|---|---|
| **Email Extraction** | Connects to each candidate's Gmail, reads emails, extracts recruiter contacts |
| **Keyword Filtering** | Blocks junk senders using a `keywords.csv` rule file (synced from DB via API) |
| **LLM Classification** | Takes raw job descriptions and classifies them as valid jobs or junk |
| **UID Tracking** | Remembers the last processed email per account so it never re-processes |
| **DuckDB Logging** | Writes a timestamped run log after every extraction run |

---

## 📦 Installation

### 1. Clone and enter the directory

```bash
cd email-extractor-bot
```

### 2. Create and activate a virtual environment

```bash
python -m venv venv

# Windows
venv\Scripts\activate

# Linux / Mac
source venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
python -m spacy download en_core_web_sm
```

### 4. Configure environment variables

```bash
cp .env.example .env
```

Open `.env` and fill in your values:

```env
# ── Whitebox Learning API (required for everything) ─────────────────
API_BASE_URL=https://whitebox-learning.com
API_EMAIL=your@email.com
API_PASSWORD=your_password
EMPLOYEE_ID=your_employee_id

# ── Test Gmail account (optional, for single-account dry runs) ───────
TEST_EMAIL=your.email@gmail.com
TEST_APP_PASSWORD=your_16_char_app_password   # Gmail App Password, NOT regular password
TEST_BATCH_SIZE=400

# ── Local database (optional, only if running db directly) ───────────
DB_HOST=localhost
DB_PORT=3306
DB_USER=root
DB_PASSWORD=
DB_NAME=automation_db

# ── Email reporting (optional, SMTP for run summary emails) ──────────
SMTP_SERVER=smtp.gmail.com
SMTP_PORT=587
SMTP_USERNAME=
SMTP_PASSWORD=
REPORT_FROM_EMAIL=
REPORT_TO_EMAIL=

# ── LLM Classification ───────────────────────────────────────────────
# Leave blank to use local Ollama (qwen2.5:1.5b)
# Fill in to use Groq cloud instead
GROQ_API_KEY=
MODEL_NAME=
```

> **Gmail App Password**: Go to [myaccount.google.com/apppasswords](https://myaccount.google.com/apppasswords). Enable 2FA first, then generate a 16-character app password. Use this — NOT your regular Gmail password.

---

## 🚀 Running the Email Extractor

The main entry point is `src/run_workflow.py`. It reads the workflow config from the database (via API), fetches all active candidates with IMAP credentials, and runs extraction for each one.

### Run for ALL candidates (normal production run)

```bash
python src/run_workflow.py --workflow-key email_extractor
```

This is the standard run. It connects to every active candidate's inbox listed in the workflow, processes new emails since the last run, extracts recruiter contacts, and saves them to the database.

---

### Run for a SINGLE candidate by ID

```bash
python src/run_workflow.py --workflow-key email_extractor --candidate-id 42
```

Useful when you want to test just one specific candidate without running the whole list. Replace `42` with the actual candidate ID from the database.

---

### Run for a SINGLE candidate by email address

```bash
python src/run_workflow.py --workflow-key email_extractor --candidate-email john.doe@gmail.com
```

Same as above but you use the candidate's email address instead of their ID.

---

### Dry run (preview without extracting anything)

```bash
python src/run_workflow.py --workflow-key email_extractor --dry-run
```

This does NOT extract any emails or save anything to the database. It runs the SQL query from the workflow config and prints out the list of candidates it would process — their email, ID, name, and whether they have an IMAP password. Great for sanity-checking before a real run.

---

### Dry run for a single candidate

```bash
python src/run_workflow.py --workflow-key email_extractor --candidate-id 42 --dry-run
```

Combines both — shows you what would happen for just that one candidate, without touching anything.

---

### Pass extra runtime parameters

```bash
python src/run_workflow.py --workflow-key email_extractor --params '{"batch_size": 50}'
```

You can pass a JSON string of extra parameters that override the workflow defaults.

---

## 🤖 Running the LLM Job Classifier

The classifier reads raw job listings from the database (status = `new`), sends each one to an LLM, and classifies it as either a **valid job** (saves to `job_listing` table, marks as `parsed`) or **junk** (just marks as `parsed`).

### Setup: Choose your LLM

**Option A — Local Ollama (recommended, no API cost)**

Start the Ollama container from a separate project:
```bash
cd ../project-Ollama-local-llm
docker-compose up -d
docker exec ollama ollama pull qwen2.5:1.5b
```

Leave `GROQ_API_KEY` blank in your `.env` and the classifier will automatically use Ollama.

**Option B — Groq Cloud (faster, uses your API key)**

Add to `.env`:
```env
GROQ_API_KEY=your_gsk_key_here
MODEL_NAME=llama-3.1-8b-instant
```

---

### Normal production run (classifies everything in batches)

```bash
python llm_based_classifier.py
```

Processes all unclassified raw jobs in batches of 100. Runs until there's nothing left.

---

### Run with a smaller batch size

```bash
python llm_based_classifier.py --batch-size 20
```

Good for testing or when you want to process slower and check logs as it goes.

---

### Dry run (preview without saving anything)

```bash
python llm_based_classifier.py --dry-run
```

Fetches one batch of raw jobs, runs them through the LLM, prints what it *would* save — but writes nothing to the database. Use this to check if your LLM connection is working and your classifications look reasonable.

---

### Dry run with custom batch size

```bash
python llm_based_classifier.py --dry-run --batch-size 5
```

Same as dry run, but only looks at the first 5 records. Quick sanity check.

---

### Adjust the confidence threshold

```bash
python llm_based_classifier.py --threshold 0.8
```

Default threshold is `0.7`. If the LLM returns a confidence score below this, the job is rejected as junk. Raise it to be stricter.

---

### What the classifier outputs

- Logs every classification to **`llm_classification.log`** (console + file)
- Writes a human-readable audit entry to **`classification_audit_llm.log`** for every record (ID, label, score, reasoning)

---

## 🔑 Keyword Filter Management

The bot uses a `keywords.csv` file at the **project root** to filter junk emails. If the file doesn't exist, it automatically falls back to fetching the rules from the API/database at runtime.

### Sync keywords from database to CSV

```bash
python scripts/sync_keywords_to_csv.py
```

Fetches all keywords from the API and writes them to `keywords.csv` in the project root. Run this whenever the keyword rules have been updated in the database so the bot picks up the latest filters.

### Preview what would change (no file written)

```bash
python scripts/sync_keywords_to_csv.py --dry-run
```

Shows what new or changed rows would be written, without actually touching the CSV file.

### Verbose output for debugging

```bash
python scripts/sync_keywords_to_csv.py --verbose
```

---

## 🔁 UID Tracker (Resetting Email History)

The bot tracks the last-processed email UID per candidate in `last_run.json`. This is how it knows which emails are "new" and avoids reprocessing.

### Reset the tracker (interactive)

```bash
python scripts/reset_tracker.py
```

This gives you a menu with options:
1. Reset a **specific account** — that account's emails will all be re-processed next run
2. Reset **all accounts** — everything gets re-processed
3. **Delete** the tracker file entirely
4. Exit with no changes

> **When to use this**: If you want to re-process emails for a candidate (e.g. to retest extraction logic), reset their entry here first.

---

## 🔍 Utilities & Diagnostics

### Check what data is in local DuckDB storage

```bash
python scripts/check_local_data.py
```

### View DuckDB run logs

```bash
python scripts/view_duckdb.py
```

Shows the timestamped run log files generated after each extraction run.

### Generate a DuckDB log manually

```bash
python scripts/generate_duckdb_log.py
```

### Diagnose an IMAP connection problem

```bash
python scripts/diagnose_account.py
```

Runs a series of checks on a Gmail account to find out why IMAP isn't connecting (wrong app password, IMAP disabled, 2FA not set up, etc.).

---

## 🏗️ How the Extraction Pipeline Works

```
Candidate Gmail (IMAP)
    ↓
Fetch new emails (since last UID in last_run.json)
    ↓
Filter — keywords.csv rules + dynamic heuristics (UUID patterns, hash strings, reply-tracking, etc.)
    ↓
Clean email body (HTML → plain text, strip signatures artifacts)
    ↓
Extract recruiter contact (Regex → SpaCy → GLiNER fallback)
    ↓  
Validate & deduplicate (by email + LinkedIn ID)
    ↓
Save to database via API
    ↓
Update last_run.json with latest UID
    ↓
Write DuckDB run log
```

---

## ⚙️ Configuration File

`configs/config.yaml` controls extraction behavior:

```yaml
extraction:
  enabled_methods:
    - regex      # Fast pattern-based extraction
    - spacy      # Named entity recognition
    - gliner     # Zero-shot NER (most powerful, slowest)
  
  block_gmail: true
  extract_multiple_contacts: true
  
  email_priority:
    - reply-to
    - sender
    - from
    - cc
    - body

gliner:
  model: urchade/gliner_base
  threshold: 0.5
  entity_labels:
    - person name
    - company name
    - location
    - job title
```

---

## 🐳 Docker

### Build the image

```bash
docker build -t email-extractor:latest .
```

### Run in production mode

```bash
docker run --env-file .env email-extractor:latest
```

---

## 🔧 Troubleshooting

### "No candidates found"
- Run with `--dry-run` first to see what SQL query the workflow uses and whether it returns anything
- Check that the workflow key (`email_extractor`) is configured and active in the database

### Emails not being fetched
- Look at `last_run.json` — the UID tracker might think everything is already processed
- Run `python scripts/reset_tracker.py` to reset the specific account

### Gmail IMAP connection failing
- Make sure IMAP is enabled: Gmail Settings → See all settings → Forwarding and POP/IMAP
- Use a 16-character **App Password**, not your regular Gmail password
- Run `python scripts/diagnose_account.py` for a step-by-step diagnosis

### LLM classifier not connecting
- For Ollama: make sure the Docker container is running (`docker ps`) and the model is pulled
- For Groq: verify `GROQ_API_KEY` is correct in `.env`
- Run with `--dry-run --batch-size 5` to quickly test the connection

### keywords.csv issues
- Make sure the file is in the **project root** (`email-extractor-bot/keywords.csv`), not inside `src/` or `scripts/`
- If missing, the bot will still work — it falls back to fetching rules from the API automatically
- To regenerate it: `python scripts/sync_keywords_to_csv.py`

---

## 📄 License

Internal project — Whitebox Learning Platform.
