"""
BigQuery Chat Orchestrator
Coordinates metadata retrieval, SQL generation, query execution, summary, and visualization
Updated with graceful error handling for payload size issues
FIXED: 0 rows is now treated as valid success, not error
"""

from metadata_retrieval_agent import MetadataRetrievalAgent
from bigquery_data_query_agent import BigQueryDataQueryAgent
from claude_summary_agent import ClaudeSummaryAgent
from visualization_recommendation_agent import VisualizationRecommendationAgent
from typing import Dict, Any, Optional
import os
from dotenv import load_dotenv

load_dotenv()


class BigQueryChatOrchestrator:
    def __init__(
        self,
        neo4j_uri: str,
        neo4j_username: str,
        neo4j_password: str,
        connection_project_id: str,
        data_project_id: str,
        dataset_id: str,
        use_vegas: bool = False,
        anthropic_api_key: str = None,
        vegas_endpoint: str = None,
        vegas_api_key: str = None
    ):
        """
        Initialize orchestrator with cross-project BigQuery setup
        
        Args:
            neo4j_uri: Neo4j connection URI
            neo4j_username: Neo4j username
            neo4j_password: Neo4j password
            connection_project_id: Project for BigQuery authentication
            data_project_id: Project where data actually lives
            dataset_id: BigQuery dataset name
            use_vegas: Whether to use VEGAS or direct API
            anthropic_api_key: Anthropic API key
            vegas_endpoint: VEGAS endpoint URL
            vegas_api_key: VEGAS API key
        """
        print("=" * 80)
        print("🚀 Initializing BigQuery Chat Orchestrator")
        print("=" * 80)
        
        # Initialize metadata agent
        self.metadata_agent = MetadataRetrievalAgent(
            neo4j_uri=neo4j_uri,
            neo4j_username=neo4j_username,
            neo4j_password=neo4j_password,
            use_vegas=use_vegas,
            anthropic_api_key=anthropic_api_key,
            vegas_endpoint=vegas_endpoint,
            vegas_api_key=vegas_api_key
        )
        
        # Initialize data query agent with cross-project support
        self.data_agent = BigQueryDataQueryAgent(
            connection_project_id=connection_project_id,
            data_project_id=data_project_id,
            dataset_id=dataset_id,
            use_vegas=use_vegas,
            anthropic_api_key=anthropic_api_key,
            vegas_endpoint=vegas_endpoint,
            vegas_api_key=vegas_api_key
        )
        
        # Initialize summary agent
        self.summary_agent = ClaudeSummaryAgent(
            use_vegas=use_vegas,
            anthropic_api_key=anthropic_api_key,
            vegas_endpoint=vegas_endpoint,
            vegas_api_key=vegas_api_key,
            vegas_use_case="your_use_case"
        )
        
        # Initialize visualization recommendation agent
        self.visualization_agent = VisualizationRecommendationAgent(
            use_vegas=use_vegas,
            anthropic_api_key=anthropic_api_key,
            vegas_endpoint=vegas_endpoint,
            vegas_api_key=vegas_api_key,
            vegas_use_case="your_use_case"
        )
        
        print("✅ All agents initialized successfully!\n")
    
    def process_question(
        self,
        user_question: str,
        execute: bool = True,
        include_summary: bool = True,
        include_visualization: bool = True
    ) -> Dict[str, Any]:
        """
        Process a natural language question through the complete pipeline
        
        Args:
            user_question: User's natural language question
            execute: If False, only generate SQL without executing
            include_summary: If True, generate AI summary of results
            include_visualization: If True, generate visualization recommendations
            
        Returns:
            Dictionary containing all steps and results
        """
        print("\n" + "=" * 80)
        print(f"❓ Question: {user_question}")
        print("=" * 80 + "\n")
        
        result = {
            "question": user_question,
            "metadata": None,
            "sql": None,
            "data": None,
            "summary": None,
            "visualization": None,
            "error": None,
            "success": False
        }
        
        try:
            # STEP 1: Retrieve relevant metadata from Neo4j
            print("📋 STEP 1: Retrieving metadata from Neo4j...")
            print("-" * 80)
            
            metadata = self.metadata_agent.retrieve_relevant_metadata(user_question)
            result["metadata"] = metadata
            
            if not metadata or not metadata.get("tables"):
                error_msg = "No relevant tables found for your question. Please try rephrasing or asking about different metrics."
                print(f"❌ {error_msg}\n")
                result["error"] = error_msg
                return result
            
            print(f"✅ Retrieved {len(metadata['tables'])} relevant tables\n")
            
            # STEP 2: Generate and optionally execute SQL
            print("💻 STEP 2: Generating BigQuery SQL...")
            print("-" * 80)
            
            query_result = self.data_agent.generate_and_execute_query(
                user_question=user_question,
                metadata=metadata,
                execute=execute
            )
            
            result["sql"] = query_result.get("sql")
            result["data"] = query_result.get("data")
            result["row_count"] = query_result.get("row_count", 0)
            
            # Check for any errors (payload validation or SQL generation/execution)
            if not query_result.get("success"):
                error_msg = query_result.get("error", "Unknown error occurred")
                user_message = query_result.get("user_message")
                
                print(f"❌ Query processing failed: {error_msg}\n")
                
                # Use user-friendly message if available (from payload validation)
                # Or use the error message directly (from SQL validation/execution)
                result["error"] = user_message if user_message else error_msg
                result["success"] = False
                return result
            
            if not execute:
                print("⏸️ Dry run mode - SQL generated but not executed\n")
                result["success"] = True
                return result
            
            # ✨ FIXED: Check if query execution had an actual error (not just 0 rows)
            # data=None means error, data=[] means 0 rows (valid success)
            if query_result.get("data") is None:
                error_msg = query_result.get("error", "Query execution failed")
                print(f"❌ {error_msg}\n")
                result["error"] = error_msg
                return result
            
            # ✨ FIXED: 0 rows is a valid result, not an error
            row_count = result['row_count']
            if row_count == 0:
                print(f"✅ Query executed successfully! No rows matched the criteria (0 rows)\n")
            else:
                print(f"✅ Query executed successfully! Retrieved {row_count} rows\n")
            
            # STEP 3: Generate AI summary (optional)
            # ✨ FIXED: Generate summary even for 0 rows
            if include_summary:
                print("📝 STEP 3: Generating AI summary...")
                print("-" * 80)
                
                try:
                    if row_count == 0:
                        # Special handling for 0 rows
                        result["summary"] = (
                            "The query executed successfully but returned no data. "
                            "This could mean:\n"
                            "• No records match the specified criteria\n"
                            "• The time period specified has no data\n"
                            "• The filters exclude all available data\n\n"
                            "Try adjusting your filters or expanding the time range."
                        )
                        print(f"✅ Summary generated (0 rows)\n")
                    else:
                        # Normal summary for data rows
                        summary = self.summary_agent.generate_summary(
                            user_question=user_question,
                            query_results=result["data"],
                            sql=result["sql"],
                            metadata=metadata,
                            row_count=result["row_count"]
                        )
                        result["summary"] = summary
                        print(f"✅ Summary generated\n")
                    
                except Exception as e:
                    print(f"⚠️ Warning: Could not generate summary: {e}")
                    if row_count == 0:
                        result["summary"] = "Query returned no data matching your criteria."
                    else:
                        result["summary"] = f"Query returned {result['row_count']} rows successfully. See data table below for details."
            
            # STEP 4: Generate visualization recommendation (optional)
            # ✨ FIXED: Skip visualization for 0 rows (nothing to visualize)
            if include_visualization:
                if row_count > 0:
                    print("🎨 STEP 4: Generating visualization recommendation...")
                    print("-" * 80)
                    
                    try:
                        visualization = self.visualization_agent.recommend_visualization(
                            question=user_question,
                            sql=result["sql"],
                            data=result["data"],
                            row_count=result["row_count"]
                        )
                        result["visualization"] = visualization
                        print(f"✅ Visualization recommendation generated\n")
                        
                    except Exception as e:
                        print(f"⚠️ Warning: Could not generate visualization: {e}")
                        result["visualization"] = {
                            "recommended_chart": "table",
                            "reasoning": "Default table view due to error",
                            "chart_config": {"title": "Query Results"}
                        }
                else:
                    print("⏭️  STEP 4: Skipping visualization (0 rows)\n")
                    result["visualization"] = None
            
            result["success"] = True
            
        except Exception as e:
            error_msg = str(e)
            print(f"❌ Pipeline error: {error_msg}\n")
            result["error"] = error_msg
            result["success"] = False
        
        return result
    
    def close(self):
        """Close all connections"""
        if hasattr(self, 'metadata_agent'):
            self.metadata_agent.close()
        print("🔒 All connections closed")


if __name__ == "__main__":
    # Test the orchestrator
    orchestrator = BigQueryChatOrchestrator(
        neo4j_uri=os.getenv("NEO4J_URI"),
        neo4j_username=os.getenv("NEO4J_USERNAME"),
        neo4j_password=os.getenv("NEO4J_PASSWORD"),
        connection_project_id=os.getenv("GCP_CONNECTION_PROJECT_ID"),
        data_project_id=os.getenv("GCP_DATA_PROJECT_ID"),
        dataset_id=os.getenv("GCP_DATASET_ID"),
        use_vegas=False,
        anthropic_api_key=os.getenv("ANTHROPIC_API_KEY")
    )
    
    try:
        # Test question
        result = orchestrator.process_question(
            user_question="What is the trend of customer sentiment over the past 6 months?",
            execute=True,
            include_summary=True,
            include_visualization=True
        )
        
        if result["success"]:
            print("\n" + "=" * 80)
            print("📊 FINAL RESULTS")
            print("=" * 80)
            print(f"\n✅ SQL:\n{result['sql']}")
            print(f"\n✅ Rows: {result['row_count']}")
            print(f"\n✅ Summary:\n{result['summary']}")
            
            if result.get("visualization"):
                print(f"\n🎨 Visualization:")
                print(f"   Chart Type: {result['visualization'].get('recommended_chart')}")
                print(f"   Reasoning: {result['visualization'].get('reasoning')}")
                if result['visualization'].get('chart_config'):
                    print(f"   Config: {result['visualization']['chart_config']}")
        else:
            print(f"\n❌ Failed: {result['error']}")
            
    finally:
        orchestrator.close()
