"""
BigQuery Data Query Agent
Generates and optionally executes BigQuery SQL queries
Updated to support cross-project setup and send only enriched metadata to LLM
FIXED VERSION: Keeps all columns, uses explicit SQL join formatting
"""

from google.cloud import bigquery
from vegas_adapter import VEGASAdapter
import json
import re
from typing import Dict, Any, List, Optional
import os
from dotenv import load_dotenv

load_dotenv()


class BigQueryDataQueryAgent:
    def __init__(
        self,
        connection_project_id: str,
        data_project_id: str,
        dataset_id: str,
        use_vegas: bool = False,
        anthropic_api_key: str = None,
        vegas_endpoint: str = None,
        vegas_api_key: str = None,
        vegas_use_case: str = "your_use_case"
    ):
        """
        Initialize BigQuery agent with cross-project support
        
        Args:
            connection_project_id: Project ID for authentication/connection
            data_project_id: Project ID where BigQuery dataset actually lives
            dataset_id: Dataset name
            use_vegas: Whether to use VEGAS or direct Anthropic API
            anthropic_api_key: API key for direct mode
            vegas_endpoint: VEGAS endpoint URL
            vegas_api_key: VEGAS API key
            vegas_use_case: VEGAS use case name
        """
        self.connection_project_id = connection_project_id
        self.data_project_id = data_project_id
        self.dataset_id = dataset_id
        
        # Connect using connection_project_id for auth
        self.client = bigquery.Client(project=connection_project_id)
        
        print(f"✅ BigQuery client initialized:")
        print(f"   Connection Project: {connection_project_id}")
        print(f"   Data Project: {data_project_id}")
        print(f"   Dataset: {dataset_id}")
        
        # LLM adapter (supports both direct API and VEGAS)
        self.llm = VEGASAdapter(
            use_vegas=use_vegas,
            anthropic_api_key=anthropic_api_key,
            vegas_endpoint=vegas_endpoint,
            vegas_api_key=vegas_api_key,
            use_case=vegas_use_case
        )
    
    def generate_and_execute_query(
        self,
        user_question: str,
        metadata: Dict[str, Any],
        ontology: Optional[Dict[str, Any]] = None,
        execute: bool = True
    ) -> Dict[str, Any]:
        """
        Generate SQL query and optionally execute it
        
        Args:
            user_question: Natural language question
            metadata: Retrieved metadata from Neo4j
            ontology: Full ontology for context
            execute: If False, only generate SQL (dry run mode)
            
        Returns:
            Dictionary with SQL, results, and execution status
        """
        print(f"💻 Generating BigQuery SQL...")
        
        # Generate SQL
        sql = self._generate_sql(user_question, metadata, ontology)
        
        print(f"📝 Generated SQL:\n{sql}\n")
        
        result = {
            "sql": sql,
            "data": None,
            "row_count": 0,
            "error": None,
            "success": False
        }
        
        # Execute if requested
        if execute:
            print("🔄 Executing SQL against BigQuery...")
            execution_result = self._execute_sql(sql)
            
            result.update(execution_result)
            
            if result["success"]:
                row_count = result["row_count"]
                print(f"✅ Query successful! Retrieved {row_count} rows")
                
                # Show sample results
                if result["data"] and len(result["data"]) > 0:
                    print(f"📊 Sample results (first 3 rows):")
                    for i, row in enumerate(result["data"][:3]):
                        print(f"   Row {i+1}: {row}")
            else:
                # Error from SQL execution (including invalid SQL detection)
                print(f"❌ Error: {result['error']}")
        else:
            print("⏸️ Dry run mode - SQL generated but not executed")
            result["success"] = True  # SQL generation succeeded
        
        return result
    
    def _simplify_metadata_for_llm(self, metadata: Dict[str, Any], max_columns: int = 200) -> Dict[str, Any]:
        """
        Prepare metadata for LLM - keep ALL columns (don't filter aggressively)
        
        KEY FIXES:
        - Keep ALL relevant tables (Neo4j already filtered them)
        - Keep ALL columns (enrichments are bonuses, not requirements!)
        - Only limit if exceeds max_columns (default 200, plenty of room)
        - Reduce sample_values and descriptions to save space
        
        This ensures important columns aren't filtered out
        """
        tables = metadata.get("tables", [])
        simplified_tables = []
        
        for table in tables:
            columns = table.get("columns", [])
            
            # Keep ALL columns - don't filter by importance!
            # Only limit if exceeds max_columns (default 200, plenty of room)
            selected_columns = columns[:max_columns]
            
            # Build column data - include only fields that exist and have values
            simplified_columns = []
            for col in selected_columns:
                if isinstance(col, dict):
                    # Start with required fields
                    col_data = {
                        "name": col.get("name", ""),
                        "type": col.get("data_type", col.get("type", ""))
                    }
                    
                    # Add optional enriched fields ONLY if they exist and have values
                    if col.get("semantic_type"):
                        col_data["semantic_type"] = col.get("semantic_type")
                    
                    if col.get("sample_values"):
                        # Limit to first 3 sample values to save space
                        col_data["sample_values"] = col.get("sample_values")
                    
                    if col.get("business_term"):
                        col_data["business_term"] = col.get("business_term")
                    
                    if col.get("business_definition"):
                        # Limit definition to 100 chars
                        col_data["business_definition"] = col.get("business_definition")[:100]
                    
                    if col.get("usage_notes"):
                        # Limit usage notes to 150 chars
                        col_data["usage_notes"] = col.get("usage_notes")[:150]
                    
                    if col.get("data_quality_note"):
                        col_data["data_quality_note"] = col.get("data_quality_note")
                    
                    if col.get("unit"):
                        col_data["unit"] = col.get("unit")
                    
                    simplified_columns.append(col_data)
                    
                elif isinstance(col, str):
                    # Just a column name string
                    simplified_columns.append({"name": col, "type": "unknown"})
            
            simplified_tables.append({
                "name": table.get("name"),
                "type": table.get("type", "table"),
                "business_description": table.get("business_description", ""),
                "columns": simplified_columns
            })
        
        return {
            "tables": simplified_tables,
            "joins": metadata.get("joins", [])[:10]  # Keep up to 10 joins
        }
    
    def _generate_sql(
        self,
        user_question: str,
        metadata: Dict[str, Any],
        ontology: Optional[Dict[str, Any]]
    ) -> str:
        """Generate SQL using LLM (via VEGAS or direct API)"""
        
        # Simplify metadata - keep ALL columns (no aggressive filtering)
        simplified_metadata = self._simplify_metadata_for_llm(metadata)
        
        orig_tables = len(metadata.get('tables', []))
        simp_tables = len(simplified_metadata.get('tables', []))
        orig_cols = sum(len(t.get('columns', [])) for t in metadata.get('tables', []))
        simp_cols = sum(len(t.get('columns', [])) for t in simplified_metadata.get('tables', []))
        
        print(f"📉 Prepared metadata: {orig_tables} tables (kept all), {orig_cols} columns → {simp_cols} columns")
        
        # Format joins as EXPLICIT SQL snippets (not just arrows!)
        joins_list = ""
        prioritized_joins = simplified_metadata.get("joins", [])
        if prioritized_joins:
            print(f"🔗 Formatting {len(prioritized_joins)} joins for SQL generation")
            joins_list = "AVAILABLE JOINS (use EXACT syntax below):\n\n"
            
            for i, join in enumerate(prioritized_joins, 1):
                from_table = join['from_table']
                to_table = join['to_table']
                on_field = join['on_field']
                
                # Create simple table aliases (first letter of first word)
                from_alias = from_table.split('_')[0][0]
                to_alias = to_table.split('_')[0][0]
                
                joins_list += f"{i}. {from_table} → {to_table}\n"
                joins_list += f"   USE THIS SQL: JOIN {to_table} {to_alias} ON {from_alias}.{on_field} = {to_alias}.{on_field}\n\n"
            
            joins_list += "❌ If the join you need is NOT listed above, query SINGLE table only.\n"
            joins_list += "❌ DO NOT make up joins! Use EXACT columns from 'USE THIS SQL' above.\n"
        else:
            joins_list = "NO JOINS AVAILABLE - Query single table only.\n"
        
        # Format metadata as JSON string
        metadata_str = json.dumps(simplified_metadata, indent=2)
        
        print(f"📦 Payload size: metadata={len(metadata_str)} chars, joins={len(joins_list)} chars")
        
        try:
            # Call LLM via adapter - USE data_project_id for SQL generation
            sql = self.llm.generate(
                context_id="sql_generator",
                variables={
                    "user_question": user_question,
                    "metadata": metadata_str,
                    "joins": joins_list,
                    "data_project_id": self.data_project_id,
                    "dataset_id": self.dataset_id
                },
                temperature=0,
                max_tokens=3000
            )
            
            # Extract SQL from response
            sql = self._extract_sql(sql)
            
            return sql
            
        except Exception as e:
            error_msg = str(e)
            
            # Handle payload rejection
            if "413" in error_msg or "Payload Too Large" in error_msg or "PAYLOAD_TOO_LARGE" in error_msg:
                print(f"❌ Payload rejected as too large")
                raise Exception(
                    "⚠️ The query context is too large for the system to process.\n\n"
                    "This usually means:\n"
                    "• The question involves too many tables or complex relationships\n"
                    "• Try narrowing to a specific time period or metric\n\n"
                    "Example: 'What was the metric in March?' instead of 'Analyze all data'"
                )
            
            # Re-raise other errors
            print(f"❌ Error generating SQL: {e}")
            raise
    
    def _extract_sql(self, response: str) -> str:
        """Extract SQL query from LLM response"""
        
        response = response.strip()
        extracted_sql = None
        
        # STEP 1: Try to extract SQL from response (ignore any commentary)
        if "```sql" in response.lower():
            # Extract from ```sql ... ```
            pattern = r"```sql\n(.*?)\n```"
            match = re.search(pattern, response, re.DOTALL | re.IGNORECASE)
            if match:
                extracted_sql = match.group(1).strip()
        
        elif "```" in response:
            # Extract from ``` ... ```
            lines = response.split("\n")
            in_code_block = False
            sql_lines = []
            
            for line in lines:
                if line.strip().startswith("```"):
                    in_code_block = not in_code_block
                    continue
                if in_code_block:
                    sql_lines.append(line)
            
            if sql_lines:
                extracted_sql = "\n".join(sql_lines).strip()
        
        else:
            # No code block - check if response itself looks like SQL
            response_upper = response.upper()
            if any(response_upper.startswith(keyword) for keyword in ['SELECT', 'WITH', 'INSERT', 'UPDATE']):
                extracted_sql = response
        
        # STEP 2: If we successfully extracted SQL, return it (ignore any warnings/comments)
        if extracted_sql:
            return extracted_sql
        
        # STEP 3: No SQL found - check if this is an error message
        response_lower = response.lower()
        
        # Check for explicit "Cannot answer" from prompt
        if "cannot answer" in response_lower:
            raise Exception(response)
        
        # Check for error indicators
        error_indicators = [
            "too large", "too broad", "too much context",
            "narrow your question", "more specific",
            "missing:", "not available", "no data available",
            "cannot generate", "unable to"
        ]
        
        # Only treat as error if NO SQL found AND contains error indicators
        if any(indicator in response_lower for indicator in error_indicators):
            raise Exception(response)
        
        # No SQL and no clear error - return response as-is and let execution validate
        # (This handles cases where SQL might be in unusual format)
        return response
    
    def _execute_sql(self, sql: str) -> Dict[str, Any]:
        """Execute SQL against BigQuery"""
        
        # Check if SQL looks valid (starts with SELECT, WITH, or --)
        sql_stripped = sql.strip()
        valid_starts = ('SELECT', 'WITH', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', '--')
        
        if not any(sql_stripped.upper().startswith(start) for start in valid_starts):
            # This doesn't look like SQL - probably a conversational response
            error_msg = (
                "The system couldn't generate valid SQL for your question. "
                "This usually means the question is outside the scope of available data. "
                "Please try asking about relevant metrics and data in your dataset."
            )
            return {
                "data": None,
                "row_count": 0,
                "success": False,
                "error": error_msg
            }
        
        try:
            # Execute query using connection_project_id for auth
            # But SQL references data_project_id.dataset_id.table_name
            query_job = self.client.query(sql)
            results = query_job.result()
            
            # Convert to list of dictionaries
            data = [dict(row) for row in results]
            
            return {
                "data": data,
                "row_count": len(data),
                "success": True,
                "error": None
            }
            
        except Exception as e:
            error_msg = str(e)
            print(f"❌ Query failed: {error_msg}")
            
            return {
                "data": None,
                "row_count": 0,
                "success": False,
                "error": error_msg
            }


if __name__ == "__main__":
    # Test the agent
    agent = BigQueryDataQueryAgent(
        connection_project_id=os.getenv("GCP_CONNECTION_PROJECT_ID"),
        data_project_id=os.getenv("GCP_DATA_PROJECT_ID"),
        dataset_id=os.getenv("GCP_DATASET_ID"),
        use_vegas=False,
        anthropic_api_key=os.getenv("ANTHROPIC_API_KEY")
    )
    
    # Mock metadata
    metadata = {
        "tables": [{
            "name": "example_table",
            "columns": [
                {"name": "metric_value", "data_type": "INT64"},
                {"name": "count", "data_type": "INT64"}
            ]
        }]
    }
    
    # Generate SQL only (dry run)
    result = agent.generate_and_execute_query(
        user_question="What is the current metric?",
        metadata=metadata,
        execute=False  # Dry run mode
    )
    
    print("\n✅ SQL Generated:")
    print(result["sql"])
