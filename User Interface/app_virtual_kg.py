"""
Virtual Knowledge Graph Streamlit App
Updated to support cross-project BigQuery setup and LLM platform integration
Includes graceful error handling for payload size issues
Displays visualization recommendations
"""

import streamlit as st
import os
import sys
from dotenv import load_dotenv

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
core_agents_dir = os.path.join(parent_dir, "Core Agents")
sys.path.insert(0, core_agents_dir)

load_dotenv()

# Page config
st.set_page_config(
    page_title="Virtual Knowledge Graph",
    page_icon="🧠",
    layout="wide"
)

st.title("🧠 Virtual Knowledge Graph - Talk2Data")
st.markdown("Ask questions about your data in natural language")

# Sidebar configuration
st.sidebar.header("⚙️ Configuration")

# Data source selection
data_source = st.sidebar.radio(
    "Data Source",
    ["BigQuery", "CSV Files"]
)

# LLM Mode selection
llm_mode = st.sidebar.radio(
    "LLM Mode",
    ["Direct API", "Enterprise Platform"],
    help="Direct API: Use Anthropic API directly. Enterprise Platform: Use enterprise LLM gateway"
)

use_enterprise = (llm_mode == "Enterprise Platform")

# Connection settings
st.sidebar.subheader("Neo4j Connection")
neo4j_uri = st.sidebar.text_input("Neo4j URI", value=os.getenv("NEO4J_URI", "bolt://localhost:7687"))
neo4j_username = st.sidebar.text_input("Username", value=os.getenv("NEO4J_USERNAME", "neo4j"))
neo4j_password = st.sidebar.text_input("Password", type="password", value=os.getenv("NEO4J_PASSWORD", ""))

if data_source == "BigQuery":
    st.sidebar.subheader("BigQuery Settings")
    
    # Cross-project setup
    st.sidebar.markdown("**Cross-Project Setup:**")
    gcp_connection_project = st.sidebar.text_input(
        "Connection Project ID (for auth)",
        value=os.getenv("GCP_CONNECTION_PROJECT_ID", ""),
        help="Project used to authenticate with BigQuery"
    )
    gcp_data_project = st.sidebar.text_input(
        "Data Project ID (where dataset lives)", 
        value=os.getenv("GCP_DATA_PROJECT_ID", ""),
        help="Project where your BigQuery dataset actually exists"
    )
    gcp_dataset_id = st.sidebar.text_input(
        "Dataset ID",
        value=os.getenv("GCP_DATASET_ID", "")
    )
    
    # Query execution toggle
    execute_queries = st.sidebar.checkbox(
        "Execute queries on BigQuery",
        value=True,
        help="If unchecked, only SQL will be generated (dry run mode)"
    )
    
    if not execute_queries:
        st.sidebar.info("⏸️ Dry run mode: SQL will be generated but not executed")

# LLM API settings
st.sidebar.subheader("LLM Configuration")

if use_enterprise:
    enterprise_endpoint = st.sidebar.text_input(
        "Enterprise LLM Endpoint",
        value=os.getenv("ENTERPRISE_LLM_ENDPOINT", ""),
        help="Enterprise LLM gateway endpoint URL"
    )
    enterprise_api_key = st.sidebar.text_input(
        "Enterprise API Key",
        type="password",
        value=os.getenv("ENTERPRISE_LLM_API_KEY", ""),
        help="API key for enterprise LLM gateway"
    )
    enterprise_use_case = st.sidebar.text_input(
        "Use Case Name",
        value=os.getenv("ENTERPRISE_USE_CASE", "talk2data"),
        help="Enterprise use case identifier"
    )
    anthropic_api_key = None
else:
    anthropic_api_key = st.sidebar.text_input(
        "Anthropic API Key",
        type="password",
        value=os.getenv("ANTHROPIC_API_KEY", ""),
        help="API key for direct Anthropic access"
    )
    enterprise_endpoint = None
    enterprise_api_key = None
    enterprise_use_case = "talk2data"

# AI Summaries toggle
enable_summary = st.sidebar.checkbox(
    "Enable AI Summaries",
    value=True,
    help="Generate natural language summaries of query results"
)

if enable_summary:
    if use_enterprise:
        st.sidebar.success("✅ AI Summaries Enabled (Enterprise)")
    else:
        st.sidebar.success("✅ AI Summaries Enabled (Claude)")

# Initialize session state
if 'orchestrator' not in st.session_state:
    st.session_state.orchestrator = None
if 'history' not in st.session_state:
    st.session_state.history = []

# Initialize button
if st.sidebar.button("🔄 Initialize System"):
    with st.spinner("Initializing..."):
        try:
            if data_source == "BigQuery":
                from bigquery_chat_orchestrator import BigQueryChatOrchestrator
                
                st.session_state.orchestrator = BigQueryChatOrchestrator(
                    neo4j_uri=neo4j_uri,
                    neo4j_username=neo4j_username,
                    neo4j_password=neo4j_password,
                    connection_project_id=gcp_connection_project,
                    data_project_id=gcp_data_project,
                    dataset_id=gcp_dataset_id,
                    use_vegas=use_enterprise,
                    anthropic_api_key=anthropic_api_key,
                    vegas_endpoint=enterprise_endpoint,
                    vegas_api_key=enterprise_api_key
                )
                
                mode_msg = "Enterprise Platform" if use_enterprise else "Direct Anthropic API"
                exec_msg = "Execution Enabled" if execute_queries else "Dry Run Mode"
                st.sidebar.success(f"✅ System initialized!\n\nMode: {mode_msg}\n{exec_msg}")
                st.sidebar.info(f"🔗 Connection: {gcp_connection_project}\n📊 Data: {gcp_data_project}")
            else:
                st.sidebar.error("CSV mode not yet implemented")
                
        except Exception as e:
            st.sidebar.error(f"❌ Initialization failed: {str(e)}")

# Main interface
if st.session_state.orchestrator is None:
    st.info("👈 Configure settings in the sidebar and click 'Initialize System'")
else:
    # Query input
    user_question = st.text_input(
        "Ask a question:",
        placeholder="e.g., What is the average response time?"
    )
    
    col1, col2 = st.columns([1, 5])
    with col1:
        ask_button = st.button("🔍 Ask", type="primary")
    with col2:
        clear_button = st.button("🗑️ Clear History")
    
    if clear_button:
        st.session_state.history = []
        st.rerun()
    
    if ask_button and user_question:
        with st.spinner("Processing your question..."):
            try:
                # Call the orchestrator
                response = st.session_state.orchestrator.process_question(
                    user_question=user_question,
                    execute=execute_queries,
                    include_summary=enable_summary
                )
                
                # Add to history
                st.session_state.history.append({
                    "question": user_question,
                    "response": response
                })
                
            except Exception as e:
                st.error(f"Error processing question: {str(e)}")
    
    # Display results
    if st.session_state.history:
        st.markdown("---")
        st.subheader("Results")
        
        # Show most recent first
        for idx, item in enumerate(reversed(st.session_state.history)):
            with st.expander(f"Q: {item['question']}", expanded=(idx == 0)):
                response = item['response']
                
                # Check for errors first
                if not response.get("success"):
                    error_msg = response.get("error")
                    
                    # Handle None or empty error messages
                    if not error_msg:
                        error_msg = "Unknown error occurred - no error message provided"
                        st.error(f"❌ **Error:** {error_msg}")
                    # Check if error message has formatting
                    elif isinstance(error_msg, str) and ("\n" in error_msg or "•" in error_msg):
                        # Display as warning with formatting preserved
                        st.warning("⚠️ **Unable to Process Question**")
                        st.markdown(error_msg)
                    else:
                        # Display as regular error
                        st.error(f"❌ **Error:** {str(error_msg)}")
                    
                    # Show partial metadata for debugging if available
                    if response.get("metadata"):
                        with st.expander("🔍 Retrieved Metadata (for debugging)", expanded=False):
                            tables = response["metadata"].get("tables", [])
                            st.write(f"System found {len(tables)} tables before encountering the issue:")
                            for table in tables:
                                st.write(f"• **{table.get('name')}** - {len(table.get('columns', []))} columns")
                    
                    continue  # Skip to next item
                
                # Success case - show all steps
                # Step 1: Show metadata
                if response.get("metadata"):
                    with st.expander("📊 Step 1: Metadata Retrieved"):
                        tables = response["metadata"].get("tables", [])
                        st.write(f"Found {len(tables)} relevant table(s):")
                        for table in tables:
                            st.write(f"• **{table.get('name')}** - {len(table.get('columns', []))} columns")
                
                # Step 2: Show SQL
                if response.get("sql"):
                    with st.expander("💻 Step 2: SQL Generated", expanded=not execute_queries):
                        st.code(response["sql"], language="sql")
                        
                        if not execute_queries:
                            st.warning("⏸️ **Dry Run Mode**: SQL was generated but not executed. Copy the SQL above and run it manually.")
                
                # Step 3 & 4: Show results (if executed)
                if execute_queries:
                    # Step 3a: Show summary first if available
                    if response.get("summary"):
                        st.success("💡 **AI Summary**")
                        st.write(response["summary"])
                        st.markdown("---")
                    
                    # Step 3b: Show visualization recommendation
                    if response.get("visualization"):
                        viz = response["visualization"]
                        
                        st.success("📊 **Visualization Recommendation**")
                        
                        # Main recommendation
                        col1, col2 = st.columns([2, 1])
                        
                        with col1:
                            chart_type = viz.get("recommended_chart", "table").replace("_", " ").title()
                            st.markdown(f"### {chart_type}")
                            st.write(f"**Why:** {viz.get('reasoning', 'No reasoning provided')}")
                        
                        with col2:
                            st.markdown("**Configuration:**")
                            x_axis = viz.get('x_axis')
                            y_axis = viz.get('y_axis')
                            group_by = viz.get('group_by')
                            
                            if x_axis:
                                st.write(f"• X-axis: `{x_axis}`")
                            if y_axis:
                                st.write(f"• Y-axis: `{y_axis}`")
                            if group_by:
                                st.write(f"• Group by: `{group_by}`")
                        
                        # Chart config details
                        if viz.get("chart_config"):
                            config = viz["chart_config"]
                            
                            with st.expander("🎨 Chart Configuration Details", expanded=False):
                                config_col1, config_col2 = st.columns(2)
                                
                                with config_col1:
                                    st.write(f"**Title:** {config.get('title', 'N/A')}")
                                    st.write(f"**Color Scheme:** {config.get('color_scheme', 'N/A')}")
                                    st.write(f"**Orientation:** {config.get('orientation', 'vertical')}")
                                
                                with config_col2:
                                    st.write(f"**Show Legend:** {config.get('show_legend', True)}")
                                    st.write(f"**Show Values:** {config.get('show_values', True)}")
                                    st.write(f"**Sort Order:** {config.get('sort_order', 'none')}")
                        
                        # Alternative charts
                        if viz.get("alternative_charts"):
                            alts = viz["alternative_charts"]
                            alt_text = ", ".join([a.replace("_", " ").title() for a in alts])
                            st.info(f"💡 **Alternative options:** {alt_text}")
                        
                        st.markdown("---")
                    
                    # Step 4: Show data table
                    if response.get("data"):
                        st.success(f"📊 **Query Results** ({response.get('row_count', 0)} rows)")
                        st.dataframe(response["data"], use_container_width=True)
                    else:
                        st.info("No data returned from query")

# Footer with tips
st.markdown("---")
st.caption("💡 Tip: Use specific time periods and metrics in your questions for best results")
