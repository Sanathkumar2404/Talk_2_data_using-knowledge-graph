# Talk2Data - Natural Language to BigQuery

A comprehensive system that allows users to query BigQuery data using natural language questions. The system uses a virtual knowledge graph (Neo4j) to understand data semantics and leverages Claude AI to generate SQL queries, summaries, and visualization recommendations.

## 🌟 Features

- **Natural Language Querying**: Ask questions in plain English about your data
- **Intelligent Metadata Retrieval**: Concept-first approach to finding relevant tables and columns
- **SQL Generation**: Automatic BigQuery SQL query generation
- **Query Execution**: Direct execution against BigQuery with cross-project support
- **AI Summaries**: Natural language summaries of query results with key insights
- **Visualization Recommendations**: Smart chart/graph suggestions based on data patterns
- **Multiple Interfaces**: 
  - Streamlit web UI
  - FastAPI REST API
- **Dual LLM Support**: Works with both direct API's and platforms

## 🏗️ Architecture
```
┌─────────────────┐
│   User Query    │
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────────────┐
│    BigQuery Chat Orchestrator           │
│  (Coordinates all agents)               │
└─────────┬───────────────────────────────┘
          │
          ├─────────────────┬──────────────┬───────────────┐
          ▼                 ▼              ▼               ▼
┌──────────────────┐ ┌──────────┐ ┌──────────┐ ┌──────────────┐
│ Metadata Agent   │ │ SQL Gen  │ │ Summary  │ │ Viz Agent    │
│ (Neo4j Search)   │ │ Agent    │ │ Agent    │ │ (Chart Rec.) │
└──────────────────┘ └──────────┘ └──────────┘ └──────────────┘
          │                 │              │               │
          ▼                 ▼              ▼               ▼
     [Neo4j]          [BigQuery]      [Claude]        [Claude]
```

## 📋 Prerequisites

- Python 3.8+
- Neo4j database with your data ontology/metadata
- Google Cloud Project with BigQuery access
- Anthropic API key OR platform access
- Required Python packages (see requirements below)

## 🚀 Installation

### 1. Clone the repository
```bash
git clone <your-repo-url>
cd talk2data
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

**Required packages:**
```
streamlit>=1.28.0
fastapi>=0.104.0
uvicorn>=0.24.0
anthropic>=0.7.0
neo4j>=5.14.0
google-cloud-bigquery>=3.13.0
python-dotenv>=1.0.0
pydantic>=2.4.0
requests>=2.31.0
```

### 3. Set up environment variables

Create a `.env` file in the project root:
```env
# Neo4j Configuration
NEO4J_URI=bolt://localhost:7687
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=your_password

# BigQuery Configuration (Cross-Project Support)
GCP_CONNECTION_PROJECT_ID=your-auth-project
GCP_DATA_PROJECT_ID=your-data-project
GCP_DATASET_ID=your_dataset

# LLM Configuration
USE_VEGAS=false  # Set to 'true' for VEGAS, 'false' for direct API

# Direct Anthropic API (if USE_VEGAS=false)
ANTHROPIC_API_KEY=your_anthropic_key

# VEGAS Platform (if USE_VEGAS=true)
VEGAS_ENDPOINT=https://your-vegas-endpoint
VEGAS_API_KEY=your_vegas_key
VEGAS_USE_CASE=talk2datatest

# API Server Configuration (optional)
API_HOST=0.0.0.0
API_PORT=8000
```

### 4. Set up Google Cloud credentials
```bash
# Set the path to your service account key
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/service-account-key.json"
```

## 💻 Usage

### Option 1: Streamlit Web Interface

The easiest way to get started:
```bash
cd "User Interface"
streamlit run app_virtual_kg.py
```

Then open your browser to `http://localhost:8501`

**Features:**
- Interactive query interface
- Real-time results display
- Configuration panel
- Query history
- Visualization recommendations

### Option 2: FastAPI REST API

For programmatic access and integration:
```bash
cd "User Interface"
python fastapi_talk2data.py
```

API will be available at `http://localhost:8000`

**Interactive API docs:** `http://localhost:8000/docs`

#### Key Endpoints

**One-Shot Complete Query** (Recommended for simple integrations)
```bash
POST /api/complete
Content-Type: application/json

{
  "question": "What is the sentiment trend over the past 6 months?",
  "execute": true,
  "include_summary": true
}
```

**Response:**
```json
{
  "session_id": "uuid-here",
  "question": "What is the sentiment trend...",
  "status": "completed",
  "metadata": {...},
  "sql": "SELECT...",
  "data": [...],
  "row_count": 100,
  "summary": "Over the past 6 months...",
  "visualization": {
    "recommended_chart": "line_chart",
    "reasoning": "...",
    "chart_config": {...}
  },
  "success": true,
  "error": null
}
```

**Session-Based Query** (For progressive retrieval)
```bash
# Step 1: Submit query
POST /api/query
{
  "question": "What is our average handle time?",
  "execute": true,
  "include_summary": true
}

# Response: {"session_id": "abc-123", ...}

# Step 2: Get results
GET /api/complete/abc-123        # Complete results
GET /api/metadata/abc-123        # Metadata only
GET /api/sql/abc-123             # SQL only
GET /api/summary/abc-123         # Summary only
GET /api/results/abc-123         # Results + Visualization
```

**Other Endpoints:**
```bash
GET  /health                     # Health check
GET  /api/sessions               # List active sessions
DELETE /api/session/{id}         # Delete session
```

## 📁 Project Structure
```
talk2data/
├── Core Agents/
│   ├── bigquery_chat_orchestrator.py         # Main coordinator
│   ├── metadata_retrieval_agent.py           # Neo4j metadata search
│   ├── bigquery_data_query_agent.py          # SQL generation & execution
│   ├── claude_summary_agent.py               # AI summaries
│   ├── visualization_recommendation_agent.py # Chart recommendations
│
├── User Interface/
│   ├── app_virtual_kg.py                     # Streamlit web app
│   └── fastapi_talk2data.py                  # REST API server
│
├── .env                                      # Environment configuration
├── requirements.txt                          # Python dependencies
└── README.md                                 # This file
```

## 🔧 Configuration

### Cross-Project BigQuery Setup

The system supports querying data across Google Cloud projects:

- **Connection Project**: Used for authentication/billing
- **Data Project**: Where your BigQuery dataset actually lives

This is configured via:
```python
connection_project_id="project-a"  # For auth
data_project_id="project-b"        # Where data lives
```

### LLM Modes

**Direct Anthropic API:**
- Use your own API key
- Full Claude Sonnet access
- Set `USE_VEGAS=false`

**VEGAS Platform:**
- Enterprise LLM gateway
- Managed prompts and contexts
- Set `USE_VEGAS=true`
- Requires VEGAS endpoint and credentials

## 🎯 How It Works

### 1. Metadata Retrieval
- **Concept-First Approach**: System identifies relevant business concepts (e.g., "Customer Sentiment", "Call Metrics")
- **Table Discovery**: Finds tables related to those concepts in Neo4j
- **Column Enrichment**: Retrieves semantic metadata (descriptions, types, sample values)
- **Relationship Mapping**: Identifies valid joins between tables

### 2. SQL Generation
- **Context Building**: Sends question + metadata to Claude
- **SQL Generation**: Claude generates BigQuery-compatible SQL
- **Validation**: Checks SQL syntax and structure
- **Cross-Project Support**: Generates fully-qualified table names

### 3. Query Execution
- **BigQuery Client**: Executes SQL against your dataset
- **Result Processing**: Converts results to structured format
- **Error Handling**: Graceful handling of execution errors

### 4. Summary & Visualization
- **AI Summary**: Claude analyzes results and generates insights
- **Chart Recommendation**: Suggests optimal visualization based on:
  - Data types (numeric, categorical, datetime)
  - Row count
  - Question intent
  - Data patterns

## 🛡️ Error Handling

The system includes comprehensive error handling:

- **No Metadata Found**: Suggests rephrasing the question
- **Payload Too Large**: Recommends narrowing the query scope
- **SQL Errors**: Provides user-friendly error messages
- **Zero Results**: Generates appropriate summary explaining why
- **Execution Failures**: Falls back gracefully with debugging info

## 📊 Sample Questions

Try asking:
```
"What is our average handle time by team?"
"Show customer sentiment trends over the past 6 months"
"How many calls required supervisor escalation last week?"
```


