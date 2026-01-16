"""
Claude Summary Agent
Generates natural language summaries of query results with insights
Updated to support both direct Anthropic API and VEGAS platform
Uses context_id="summary" in VEGAS use case
"""

from vegas_adapter import VEGASAdapter
import json
from typing import Any, Dict, Optional, List
import datetime


class ClaudeSummaryAgent:
    def __init__(
        self,
        use_vegas: bool = False,
        anthropic_api_key: str = None,
        vegas_endpoint: str = None,
        vegas_api_key: str = None,
        vegas_use_case: str = "your_use_case"
    ):
        """
        Initialize summary agent with VEGAS or direct API support
        
        Args:
            use_vegas: Whether to use VEGAS platform
            anthropic_api_key: API key for direct Anthropic access
            vegas_endpoint: VEGAS endpoint URL
            vegas_api_key: VEGAS API key
            vegas_use_case: VEGAS use case (default: "your_use_case")
        """
        # LLM adapter (supports both direct API and VEGAS)
        self.llm = VEGASAdapter(
            use_vegas=use_vegas,
            anthropic_api_key=anthropic_api_key,
            vegas_endpoint=vegas_endpoint,
            vegas_api_key=vegas_api_key,
            use_case=vegas_use_case
        )
    
    def generate_summary(
        self,
        user_question: str,
        query_results: Any,
        sql: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        row_count: Optional[int] = None
    ) -> str:
        """
        Generate natural language summary with insights from query results
        
        Args:
            user_question: Original user question
            query_results: Query results (list of dicts from BigQuery)
            sql: The SQL query that was executed (for context)
            metadata: Metadata about tables/columns/joins used
            row_count: Number of rows returned
            
        Returns:
            Natural language summary with insights (3-5 sentences)
        """
        print(f"📝 Generating summary for {row_count or 'unknown'} rows...")
        
        # Format results for LLM
        results_str = self._format_results(query_results, row_count)
        
        # Format metadata with rich context
        metadata_str = self._format_metadata_context(metadata) if metadata else "No metadata available"
        
        # Format SQL (optional but helpful)
        sql_str = sql if sql else "SQL not provided"
        
        try:
            # Call LLM via VEGAS adapter with context_id="summary"
            summary = self.llm.generate(
                context_id="summary",
                variables={
                    "user_question": user_question,
                    "query_results": results_str,
                    "metadata_context": metadata_str,
                    "sql_query": sql_str,
                    "row_count": str(row_count or len(query_results) if isinstance(query_results, list) else 0)
                },
                temperature=0.3,
                max_tokens=500
            )
            
            print(f"✅ Summary generated successfully")
            return summary.strip()
            
        except Exception as e:
            print(f"❌ Error generating summary: {e}")
            # Graceful fallback
            return self._generate_fallback_summary(user_question, query_results, row_count)
    
    def _format_results(self, data: Any, row_count: Optional[int] = None) -> str:
        """
        Format query results for LLM with proper context
        Includes first 10 rows for analysis
        """
        def convert_to_serializable(obj):
            """Convert non-JSON-serializable objects to strings"""
            if isinstance(obj, (datetime.date, datetime.datetime)):
                return obj.isoformat()
            elif isinstance(obj, dict):
                return {k: convert_to_serializable(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_to_serializable(item) for item in obj]
            else:
                return obj
        
        # Handle empty results
        if not data:
            return "No data returned (0 rows)"
        
        # Handle list of dicts (standard BigQuery format)
        if isinstance(data, list):
            data = convert_to_serializable(data)
            total_rows = row_count or len(data)
            
            # Show first 10 rows for analysis (more than 5 for better insights)
            preview_count = min(10, len(data))
            preview = data[:preview_count]
            
            formatted = f"Total rows: {total_rows}\n\n"
            formatted += f"Data preview ({preview_count} rows):\n"
            formatted += json.dumps(preview, indent=2)
            
            if len(data) > preview_count:
                formatted += f"\n\n(... and {len(data) - preview_count} more rows)"
            
            return formatted
        
        # Handle single dict result
        elif isinstance(data, dict):
            data = convert_to_serializable(data)
            return f"Single result:\n{json.dumps(data, indent=2)}"
        
        # Handle DataFrame (if pandas)
        elif hasattr(data, 'to_dict'):
            data_list = data.to_dict('records')
            return self._format_results(data_list, row_count)
        
        # Fallback
        else:
            return f"Data: {str(data)[:500]}"  # Truncate if very long
    
    def _format_metadata_context(self, metadata: Dict[str, Any]) -> str:
        """
        Format metadata with tables, columns, and relationships
        This gives the LLM context about the data structure
        """
        if not metadata:
            return "No metadata available"
        
        formatted = "Data Structure Context:\n\n"
        
        # Tables and columns used
        tables = metadata.get("tables", [])
        if tables:
            formatted += f"Tables used ({len(tables)}):\n"
            for table in tables:
                table_name = table.get("name", "Unknown")
                table_type = table.get("type", "table")
                columns = table.get("columns", [])
                
                formatted += f"  • {table_name} ({table_type}): {len(columns)} columns\n"
                
                # Show important columns
                important_cols = [c.get("name") for c in columns[:5] if isinstance(c, dict)]
                if important_cols:
                    formatted += f"    Key columns: {', '.join(important_cols)}\n"
        
        # Relationships/joins
        joins = metadata.get("joins", [])
        if joins:
            formatted += f"\nRelationships ({len(joins)}):\n"
            for join in joins[:5]:  # Show first 5 joins
                from_table = join.get("from_table", "?")
                to_table = join.get("to_table", "?")
                on_field = join.get("on_field", "?")
                formatted += f"  • {from_table} → {to_table} (via {on_field})\n"
        
        return formatted
    
    def _generate_fallback_summary(
        self,
        user_question: str,
        query_results: Any,
        row_count: Optional[int] = None
    ) -> str:
        """
        Generate a simple fallback summary if LLM fails
        """
        total_rows = row_count or (len(query_results) if isinstance(query_results, list) else 1)
        
        if total_rows == 0:
            return f"No data found for your question: '{user_question}'"
        elif total_rows == 1:
            return f"Found 1 result for your question: '{user_question}'. See the data table below for details."
        else:
            return f"Query returned {total_rows} rows for your question: '{user_question}'. See the data table below for details."


if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    # Test the agent
    agent = ClaudeSummaryAgent(
        use_vegas=True,
        vegas_endpoint=os.getenv("VEGAS_ENDPOINT"),
        vegas_api_key=os.getenv("VEGAS_API_KEY"),
        vegas_use_case="your_use_case"
    )
    
    # Mock query results
    results = [
        {"month": "2025-06", "total_items": 1808694, "metric_a": 30.28, "metric_b": 37.25},
        {"month": "2025-07", "total_items": 4782058, "metric_a": 28.16, "metric_b": 38.44},
        {"month": "2025-08", "total_items": 4936146, "metric_a": 28.13, "metric_b": 38.51}
    ]
    
    # Mock metadata
    metadata = {
        "tables": [
            {
                "name": "example_data_table",
                "type": "fact",
                "columns": [
                    {"name": "date_field", "type": "DATE"},
                    {"name": "category_field", "type": "STRING"},
                    {"name": "id_field", "type": "STRING"}
                ]
            }
        ],
        "joins": []
    }
    
    # Mock SQL
    sql = """
    SELECT 
      FORMAT_DATE('%Y-%m', date_field) AS month,
      COUNT(*) AS total_items,
      ROUND(SUM(CASE WHEN category_field = 'type_a' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS metric_a
    FROM example_data_table
    WHERE date_field >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)
    GROUP BY month
    """
    
    summary = agent.generate_summary(
        user_question="What is the trend over the past 6 months?",
        query_results=results,
        sql=sql,
        metadata=metadata,
        row_count=3
    )
    
    print("\n✅ Generated Summary:")
    print(summary)
