"""
FastAPI Backend for Talk2Data Virtual Knowledge Graph
Provides RESTful APIs for metadata, SQL, results, summary, and visualization
"""

from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
import os
import sys
from dotenv import load_dotenv
import uuid
from datetime import datetime

# Add Core Agents to path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
core_agents_dir = os.path.join(parent_dir, "Core Agents")
sys.path.insert(0, core_agents_dir)

from bigquery_chat_orchestrator import BigQueryChatOrchestrator

load_dotenv()

# Initialize FastAPI app
app = FastAPI(
    title="Talk2Data API",
    description="Virtual Knowledge Graph API for natural language queries to BigQuery",
    version="1.0.0"
)

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global orchestrator instance
orchestrator = None

# In-memory session storage (use Redis/database for production)
query_sessions = {}


# ============================================================================
# Pydantic Models
# ============================================================================

class QueryRequest(BaseModel):
    """Request model for asking a question"""
    question: str = Field(..., description="Natural language question", example="What is the trend over the past 6 months?")
    execute: bool = Field(True, description="Execute SQL on BigQuery or just generate")
    include_summary: bool = Field(True, description="Generate AI summary of results")


class QueryResponse(BaseModel):
    """Response model for query submission"""
    session_id: str = Field(..., description="Unique session ID for this query")
    question: str = Field(..., description="The question asked")
    status: str = Field(..., description="Query status: processing, completed, failed")
    message: str = Field(..., description="Status message")


class MetadataResponse(BaseModel):
    """Response model for metadata endpoint"""
    session_id: str
    question: str
    metadata: Optional[Dict[str, Any]] = None
    tables_count: int = 0
    success: bool
    error: Optional[str] = None


class SQLResponse(BaseModel):
    """Response model for SQL endpoint"""
    session_id: str
    question: str
    sql: Optional[str] = None
    success: bool
    error: Optional[str] = None


class SummaryResponse(BaseModel):
    """Response model for summary endpoint - SUMMARY ONLY"""
    session_id: str
    question: str
    summary: Optional[str] = None
    success: bool
    error: Optional[str] = None


class ResultsWithVizResponse(BaseModel):
    """Response model for results endpoint - RESULTS + VISUALIZATION"""
    session_id: str
    question: str
    data: Optional[List[Dict[str, Any]]] = None
    row_count: int = 0
    visualization: Optional[Dict[str, Any]] = None
    success: bool
    error: Optional[str] = None


class CompleteResponse(BaseModel):
    """Comprehensive response model with all query details"""
    session_id: str
    question: str
    status: str
    
    # Step 1: Metadata
    metadata: Optional[Dict[str, Any]] = None
    tables_count: int = 0
    
    # Step 2: SQL
    sql: Optional[str] = None
    
    # Step 3: Results
    data: Optional[List[Dict[str, Any]]] = None
    row_count: int = 0
    
    # Step 4: Summary & Visualization
    summary: Optional[str] = None
    visualization: Optional[Dict[str, Any]] = None
    
    # Overall
    success: bool
    error: Optional[str] = None
    executed: bool
    timestamp: str


class HealthResponse(BaseModel):
    """Health check response"""
    status: str
    timestamp: str
    orchestrator_initialized: bool


# ============================================================================
# Startup/Shutdown Events
# ============================================================================

@app.on_event("startup")
async def startup_event():
    """Initialize the orchestrator on startup"""
    global orchestrator
    
    print("=" * 80)
    print("🚀 Initializing Talk2Data FastAPI Server")
    print("=" * 80)
    
    try:
        orchestrator = BigQueryChatOrchestrator(
            neo4j_uri=os.getenv("NEO4J_URI"),
            neo4j_username=os.getenv("NEO4J_USERNAME"),
            neo4j_password=os.getenv("NEO4J_PASSWORD"),
            connection_project_id=os.getenv("GCP_CONNECTION_PROJECT_ID"),
            data_project_id=os.getenv("GCP_DATA_PROJECT_ID"),
            dataset_id=os.getenv("GCP_DATASET_ID"),
            use_vegas=os.getenv("USE_ENTERPRISE_LLM", "false").lower() == "true",
            anthropic_api_key=os.getenv("ANTHROPIC_API_KEY"),
            vegas_endpoint=os.getenv("ENTERPRISE_LLM_ENDPOINT"),
            vegas_api_key=os.getenv("ENTERPRISE_LLM_API_KEY")
        )
        print("✅ Orchestrator initialized successfully!")
        print("=" * 80)
    except Exception as e:
        print(f"❌ Failed to initialize orchestrator: {e}")
        print("=" * 80)
        raise


@app.on_event("shutdown")
async def shutdown_event():
    """Clean up on shutdown"""
    global orchestrator
    if orchestrator:
        orchestrator.close()
        print("🔒 Orchestrator connections closed")


# ============================================================================
# API Endpoints
# ============================================================================

@app.get("/", tags=["Health"])
async def root():
    """Root endpoint - API information"""
    return {
        "message": "Talk2Data Virtual Knowledge Graph API",
        "version": "1.0.0",
        "endpoints": {
            "health": "/health",
            "complete_oneshot": "/api/complete (POST) - One-shot: submit question, get full results immediately",
            "query": "/api/query (POST) - Submit query, get session_id for later retrieval",
            "complete_session": "/api/complete/{session_id} (GET) - Get all results for a session",
            "metadata": "/api/metadata/{session_id}",
            "sql": "/api/sql/{session_id}",
            "summary": "/api/summary/{session_id} - Get AI summary only",
            "results": "/api/results/{session_id} - Get results with visualization recommendation",
            "sessions": "/api/sessions"
        }
    }


@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check():
    """Health check endpoint"""
    return HealthResponse(
        status="healthy" if orchestrator else "unhealthy",
        timestamp=datetime.now().isoformat(),
        orchestrator_initialized=orchestrator is not None
    )


@app.post("/api/complete", response_model=CompleteResponse, tags=["Query"])
async def complete_query_oneshot(request: QueryRequest):
    """
    ONE-SHOT COMPLETE API
    
    Submit a question and get full results immediately in a single call.
    No session management required - just send question, get everything back.
    
    This endpoint:
    - Retrieves metadata from Neo4j
    - Generates SQL query
    - Executes on BigQuery (if execute=True)
    - Creates AI summary (if include_summary=True)
    - Returns visualization recommendations
    
    Perfect for simple integrations that want everything in one request/response.
    """
    if not orchestrator:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Orchestrator not initialized"
        )
    
    # Generate session ID for tracking (but not stored in sessions)
    session_id = str(uuid.uuid4())
    
    try:
        print(f"\n🔍 One-shot query (tracking: {session_id})")
        print(f"   Question: {request.question}")
        
        # Process everything synchronously
        result = orchestrator.process_question(
            user_question=request.question,
            execute=request.execute,
            include_summary=request.include_summary
        )
        
        # Extract all components
        metadata = result.get("metadata")
        sql = result.get("sql")
        data = result.get("data")
        row_count = result.get("row_count", 0)
        summary = result.get("summary")
        visualization = result.get("visualization")
        success = result.get("success", False)
        error = result.get("error")
        
        # Determine status
        if success:
            status_msg = "completed"
        elif error:
            status_msg = "failed"
        else:
            status_msg = "unknown"
        
        print(f"✅ One-shot query {status_msg}\n")
        
        return CompleteResponse(
            session_id=session_id,
            question=request.question,
            status=status_msg,
            metadata=metadata,
            tables_count=len(metadata.get("tables", [])) if metadata else 0,
            sql=sql,
            data=data,
            row_count=row_count,
            summary=summary,
            visualization=visualization,
            success=success,
            error=error,
            executed=request.execute,
            timestamp=datetime.now().isoformat()
        )
        
    except Exception as e:
        print(f"❌ One-shot query error: {e}\n")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error processing query: {str(e)}"
        )


@app.post("/api/query", response_model=QueryResponse, tags=["Query"])
async def submit_query(request: QueryRequest):
    """
    Submit a natural language query for processing (SESSION-BASED)
    
    This endpoint initiates query processing and returns a session_id.
    Use the session_id to retrieve results from other endpoints.
    
    Use this for:
    - Progressive result retrieval (get SQL first, then results later)
    - Storing results for later access
    - Multiple clients accessing same query results
    
    For simple one-shot queries, use POST /api/complete instead.
    """
    if not orchestrator:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Orchestrator not initialized"
        )
    
    # Generate unique session ID
    session_id = str(uuid.uuid4())
    
    try:
        # Process the question through orchestrator
        print(f"\n🔍 Processing query (session: {session_id})")
        print(f"   Question: {request.question}")
        
        result = orchestrator.process_question(
            user_question=request.question,
            execute=request.execute,
            include_summary=request.include_summary
        )
        
        # Store in session
        query_sessions[session_id] = {
            "question": request.question,
            "result": result,
            "timestamp": datetime.now().isoformat(),
            "execute": request.execute,
            "include_summary": request.include_summary
        }
        
        # Determine status
        if result.get("success"):
            status_msg = "completed"
            message = "Query processed successfully"
        else:
            status_msg = "failed"
            message = result.get("error", "Query processing failed")
        
        print(f"✅ Query {status_msg}: {session_id}\n")
        
        return QueryResponse(
            session_id=session_id,
            question=request.question,
            status=status_msg,
            message=message
        )
        
    except Exception as e:
        print(f"❌ Query processing error: {e}\n")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error processing query: {str(e)}"
        )


@app.get("/api/metadata/{session_id}", response_model=MetadataResponse, tags=["Results"])
async def get_metadata(session_id: str):
    """
    Get metadata retrieved from Neo4j for a query
    
    Returns:
    - Tables found
    - Columns per table
    - Relationships/joins
    """
    if session_id not in query_sessions:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Session {session_id} not found"
        )
    
    session = query_sessions[session_id]
    result = session["result"]
    
    metadata = result.get("metadata")
    tables_count = len(metadata.get("tables", [])) if metadata else 0
    
    return MetadataResponse(
        session_id=session_id,
        question=session["question"],
        metadata=metadata,
        tables_count=tables_count,
        success=metadata is not None,
        error=result.get("error") if not metadata else None
    )


@app.get("/api/sql/{session_id}", response_model=SQLResponse, tags=["Results"])
async def get_sql(session_id: str):
    """
    Get generated SQL query
    
    Returns:
    - BigQuery SQL statement
    - Query generation status
    """
    if session_id not in query_sessions:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Session {session_id} not found"
        )
    
    session = query_sessions[session_id]
    result = session["result"]
    
    sql = result.get("sql")
    
    return SQLResponse(
        session_id=session_id,
        question=session["question"],
        sql=sql,
        success=sql is not None,
        error=result.get("error") if not sql else None
    )


@app.get("/api/summary/{session_id}", response_model=SummaryResponse, tags=["Results"])
async def get_summary(session_id: str):
    """
    Get AI-generated summary ONLY
    
    Returns:
    - Natural language summary with insights
    - Key findings from the data
    
    NOTE: This endpoint returns ONLY the summary.
    For visualization recommendations, use /api/results/{session_id}
    """
    if session_id not in query_sessions:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Session {session_id} not found"
        )
    
    session = query_sessions[session_id]
    result = session["result"]
    
    summary = result.get("summary")
    
    # Check if summary was requested
    if not session["include_summary"]:
        return SummaryResponse(
            session_id=session_id,
            question=session["question"],
            summary=None,
            success=False,
            error="Summary was not requested"
        )
    
    # Check if query was executed
    if not session["execute"]:
        return SummaryResponse(
            session_id=session_id,
            question=session["question"],
            summary=None,
            success=False,
            error="Query was not executed (dry run mode)"
        )
    
    return SummaryResponse(
        session_id=session_id,
        question=session["question"],
        summary=summary,
        success=summary is not None,
        error=result.get("error") if not summary else None
    )


@app.get("/api/results/{session_id}", response_model=ResultsWithVizResponse, tags=["Results"])
async def get_results_with_visualization(session_id: str):
    """
    Get query execution results WITH visualization recommendation
    
    Returns:
    - Data rows from BigQuery
    - Row count
    - Visualization recommendation (chart type, config, reasoning)
    - Alternative chart options
    - Execution status
    
    NOTE: This endpoint returns BOTH results AND visualization.
    For summary only, use /api/summary/{session_id}
    """
    if session_id not in query_sessions:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Session {session_id} not found"
        )
    
    session = query_sessions[session_id]
    result = session["result"]
    
    data = result.get("data")
    row_count = result.get("row_count", 0)
    visualization = result.get("visualization")
    
    # Check if query was executed
    if not session["execute"]:
        return ResultsWithVizResponse(
            session_id=session_id,
            question=session["question"],
            data=None,
            row_count=0,
            visualization=None,
            success=False,
            error="Query was not executed (dry run mode)"
        )
    
    return ResultsWithVizResponse(
        session_id=session_id,
        question=session["question"],
        data=data,
        row_count=row_count,
        visualization=visualization,
        success=data is not None,
        error=result.get("error") if not data else None
    )


@app.get("/api/complete/{session_id}", response_model=CompleteResponse, tags=["Results"])
async def get_complete_results(session_id: str):
    """
    Get complete query results for a SESSION
    
    This endpoint returns everything for a previously submitted query (via POST /api/query):
    - Metadata (tables, columns, joins)
    - Generated SQL query
    - Query execution results (data rows)
    - AI-generated summary
    - Visualization recommendations
    
    Use this to retrieve full results after calling POST /api/query.
    For one-shot queries without sessions, use POST /api/complete instead.
    """
    if session_id not in query_sessions:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Session {session_id} not found"
        )
    
    session = query_sessions[session_id]
    result = session["result"]
    
    # Extract all components
    metadata = result.get("metadata")
    sql = result.get("sql")
    data = result.get("data")
    row_count = result.get("row_count", 0)
    summary = result.get("summary")
    visualization = result.get("visualization")
    success = result.get("success", False)
    error = result.get("error")
    
    # Determine status
    if success:
        status_msg = "completed"
    elif error:
        status_msg = "failed"
    else:
        status_msg = "processing"
    
    return CompleteResponse(
        session_id=session_id,
        question=session["question"],
        status=status_msg,
        metadata=metadata,
        tables_count=len(metadata.get("tables", [])) if metadata else 0,
        sql=sql,
        data=data,
        row_count=row_count,
        summary=summary,
        visualization=visualization,
        success=success,
        error=error,
        executed=session["execute"],
        timestamp=session["timestamp"]
    )


@app.delete("/api/session/{session_id}", tags=["Session Management"])
async def delete_session(session_id: str):
    """
    Delete a query session
    
    Removes session data from memory.
    Use this to clean up after retrieving results.
    """
    if session_id not in query_sessions:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Session {session_id} not found"
        )
    
    del query_sessions[session_id]
    
    return {
        "message": f"Session {session_id} deleted successfully"
    }


@app.get("/api/sessions", tags=["Session Management"])
async def list_sessions():
    """
    List all active sessions
    
    Returns session IDs and timestamps for debugging.
    """
    sessions = []
    for session_id, data in query_sessions.items():
        sessions.append({
            "session_id": session_id,
            "question": data["question"],
            "timestamp": data["timestamp"],
            "success": data["result"].get("success")
        })
    
    return {
        "total_sessions": len(sessions),
        "sessions": sessions
    }


# ============================================================================
# Error Handlers
# ============================================================================

@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Global exception handler"""
    print(f"❌ Unhandled exception: {exc}")
    return {
        "error": "Internal server error",
        "detail": str(exc)
    }


# ============================================================================
# Run Server
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    
    # Get host and port from environment or use defaults
    host = os.getenv("API_HOST", "0.0.0.0")
    port = int(os.getenv("API_PORT", "8000"))
    
    print(f"\n🚀 Starting Talk2Data API Server")
    print(f"   Host: {host}")
    print(f"   Port: {port}")
    print(f"   Docs: http://{host}:{port}/docs")
    print(f"   Redoc: http://{host}:{port}/redoc\n")
    
    uvicorn.run(
        "fastapi_talk2data:app",
        host=host,
        port=port,
        reload=True,  # Auto-reload on code changes (disable in production)
        log_level="info"
    )
