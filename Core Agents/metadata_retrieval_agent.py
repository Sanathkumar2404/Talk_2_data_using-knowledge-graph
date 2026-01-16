"""
Enhanced Metadata Retrieval Agent with Concept-First Approach
Hierarchy: Concepts → Tables → Columns
"""

from neo4j import GraphDatabase
from vegas_adapter import VEGASAdapter
import json
from typing import Dict, Any, List
import os
from dotenv import load_dotenv

load_dotenv()


class MetadataRetrievalAgent:
    def __init__(
        self, 
        neo4j_uri: str,
        neo4j_username: str,
        neo4j_password: str,
        use_vegas: bool = False,
        anthropic_api_key: str = None,
        vegas_endpoint: str = None,
        vegas_api_key: str = None,
        vegas_use_case: str = "your_use_case"
    ):
        # Neo4j connection
        self.driver = GraphDatabase.driver(
            neo4j_uri, 
            auth=(neo4j_username, neo4j_password)
        )
        
        # LLM adapter
        self.llm = VEGASAdapter(
            use_vegas=use_vegas,
            anthropic_api_key=anthropic_api_key,
            vegas_endpoint=vegas_endpoint,
            vegas_api_key=vegas_api_key,
            use_case=vegas_use_case
        )
        
    def retrieve_relevant_metadata(self, user_question: str) -> Dict[str, Any]:
        """
        Retrieve relevant metadata using concept-first approach
        
        Args:
            user_question: Natural language question
            
        Returns:
            Dictionary with concepts, tables, columns, and relationships
        """
        print(f"🔍 Searching metadata for: '{user_question}'")
        
        # Step 1: Get concept context (high-level business concepts)
        concept_context = self._get_concept_context()
        
        # Step 2: Identify relevant concepts for the question
        relevant_concepts = self._identify_relevant_concepts(
            user_question, 
            concept_context
        )
        
        if relevant_concepts:
            print(f"💡 Found {len(relevant_concepts)} relevant concepts: {[c['name'] for c in relevant_concepts]}")
        
        # Step 3: Get table context filtered by concepts
        schema_context = self._get_schema_context(relevant_concepts)
        
        # Step 4: Generate Cypher query for column-level retrieval
        cypher_query = self._generate_cypher_query(
            user_question, 
            schema_context,
            relevant_concepts
        )
        
        print(f"📝 Generated Cypher:\n{cypher_query}\n")
        
        # Step 5: Execute Cypher query
        metadata = self._execute_cypher(cypher_query)
        
        # Step 6: Add concept information to metadata
        metadata["concepts"] = relevant_concepts
        
        # Step 7: Prioritize joins based on question relevance
        if metadata.get("joins"):
            metadata["joins"] = self._prioritize_joins(
                metadata["joins"], 
                user_question
            )
        
        return metadata
    
    def _get_concept_context(self) -> List[Dict[str, Any]]:
        """Get all available business concepts"""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (c:Concept)
                OPTIONAL MATCH (c)-[:RELATES_TO]->(t:Table)
                WITH c, count(t) as table_count
                RETURN c.name as name,
                       c.definition as description,
                       table_count
                ORDER BY c.name
            """)
            
            concepts = []
            for record in result:
                concepts.append({
                    "name": record["name"],
                    "description": record.get("description", ""),
                    "keywords": [],  # Keep for backward compatibility
                    "table_count": record.get("table_count", 0)
                })
            
            return concepts
    
    def _identify_relevant_concepts(
        self, 
        user_question: str, 
        concept_context: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Use LLM to identify which concepts are relevant to the question
        
        Args:
            user_question: The user's question
            concept_context: List of all available concepts
            
        Returns:
            List of relevant concepts
        """
        if not concept_context:
            return []
        
        # Format concepts for LLM - just names and descriptions
        concepts_str = "\n".join([
            f"- {c['name']}: {c['description']}"
            for c in concept_context
        ])
        
        try:
            response = self.llm.generate(
                context_id="concept_identifier",
                variables={
                    "prompt": user_question,
                    "concepts_list": concepts_str
                },
                temperature=0,
                max_tokens=500
            )
            
            # Parse JSON response
            response = response.strip()
            
            # Remove markdown if present
            if response.startswith("```json"):
                response = response.split("\n", 1)[1].rsplit("\n", 1)[0]
            elif response.startswith("```"):
                response = response.split("\n", 1)[1].rsplit("\n", 1)[0]
            
            # Remove any trailing text after the JSON array
            if "]" in response:
                response = response[:response.rindex("]")+1]
            
            response = response.strip()
            
            # Parse JSON
            concept_names = json.loads(response)
            
            # Validate it's a list
            if not isinstance(concept_names, list):
                raise ValueError(f"Expected list, got {type(concept_names)}")
            
            # Get full concept objects
            relevant = [
                c for c in concept_context 
                if c["name"] in concept_names
            ]
            
            print(f"✅ LLM identified {len(relevant)} concepts: {[c['name'] for c in relevant]}")
            return relevant
            
        except Exception as e:
            print(f"⚠️ Error identifying concepts: {e}")
            if 'response' in locals():
                print(f"   Response was: '{response[:200]}'")
            print("🔄 Falling back to keyword matching...")
            
            # IMPROVED fallback: Better keyword matching
            question_lower = user_question.lower()
            question_words = set(question_lower.split())
            relevant = []
            
            for concept in concept_context:
                score = 0
                
                # Check if concept name words are in question
                concept_words = set(concept["name"].lower().split())
                word_matches = question_words & concept_words
                score += len(word_matches) * 10
                
                # Check full concept name as phrase
                if concept["name"].lower() in question_lower:
                    score += 15
                
                # Check description words (only meaningful words)
                if concept.get("description"):
                    desc_words = set(concept["description"].lower().split())
                    desc_matches = question_words & desc_words
                    score += len(desc_matches) * 2
                
                if score > 5:
                    relevant.append({**concept, "relevance_score": score})
            
            # Sort by score and take top 5
            relevant.sort(key=lambda x: x.get("relevance_score", 0), reverse=True)
            top_concepts = relevant[:5]
            
            print(f"✅ Fallback identified {len(top_concepts)} concepts: {[c['name'] for c in top_concepts]}")
            return top_concepts
    
    def _get_schema_context(
        self, 
        relevant_concepts: List[Dict[str, Any]] = None
    ) -> str:
        """
        Get table overview, filtered by concepts if provided
        
        Args:
            relevant_concepts: Optional list of concepts to filter by
            
        Returns:
            Formatted string of available tables
        """
        with self.driver.session() as session:
            if relevant_concepts:
                # Get tables related to identified concepts
                concept_names = [c["name"] for c in relevant_concepts]
                result = session.run("""
                    MATCH (c:Concept)-[r:RELATES_TO]->(t:Table)
                    WHERE c.name IN $concept_names
                    RETURN DISTINCT t.name as name, 
                           t.business_description as description,
                           c.name as concept,
                           r.confidence as confidence
                    ORDER BY 
                        CASE r.confidence 
                            WHEN 'high' THEN 1 
                            WHEN 'medium' THEN 2 
                            ELSE 3 
                        END,
                        c.name
                    LIMIT 60
                """, concept_names=concept_names)
            else:
                # Get all tables
                result = session.run("""
                    MATCH (t:Table)
                    OPTIONAL MATCH (c:Concept)-[:RELATES_TO]->(t)
                    RETURN t.name as name, 
                           t.business_description as description,
                           c.name as concept,
                           null as confidence
                    LIMIT 60
                """)
            
            tables = []
            for record in result:
                concept = record.get('concept', 'Uncategorized')
                confidence = record.get('confidence', '')
                confidence_tag = f" [{confidence}]" if confidence else ""
                tables.append(
                    f"- [{concept}]{confidence_tag} {record['name']}: {record.get('description', 'N/A')}"
                )
            
            return "\n".join(tables)
    
    def _generate_cypher_query(
        self, 
        user_question: str, 
        schema_context: str,
        relevant_concepts: List[Dict[str, Any]] = None
    ) -> str:
        """Generate Cypher query using LLM with concept context"""
        
        # Build concept hint for the prompt
        concept_hint = ""
        if relevant_concepts:
            concept_names = [c["name"] for c in relevant_concepts]
            concept_hint = f"\n**RELEVANT CONCEPTS:** {', '.join(concept_names)}\n"
            concept_hint += "Prioritize tables and columns related to these concepts.\n"
        
        try:
            cypher = self.llm.generate(
                context_id="metadata_generator",
                variables={
                    "user_question": user_question,
                    "schema_context": schema_context,
                    "concept_hint": concept_hint
                },
                temperature=0,
                max_tokens=2000
            )
            
            # Clean up response
            cypher = cypher.strip()
            if cypher.startswith("```cypher"):
                lines = cypher.split("\n")
                cypher = "\n".join(lines[1:-1])
            elif cypher.startswith("```"):
                lines = cypher.split("\n")
                cypher = "\n".join(lines[1:-1])
            
            return cypher
            
        except Exception as e:
            print(f"❌ Error generating Cypher: {e}")
            raise
    
    def _prioritize_joins(self, joins: list, user_question: str) -> list:
        """Score and prioritize joins based on relevance to the question"""
        if not joins:
            return []
        
        scored_joins = []
        question_lower = user_question.lower()
        
        dimension_keywords = {
            'agent': ['agent', 'representative', 'rep'],
            'customer': ['customer', 'cust', 'subscriber'],
            'call': ['call', 'interaction', 'contact'],
            'center': ['center', 'location'],
            'device': ['device', 'phone', 'equipment']
        }
        
        for join in joins:
            score = 0
            
            # Handle on_field as list
            on_fields = join['on_field'] if isinstance(join['on_field'], list) else [join['on_field']]
            
            # Check each field for relevance
            for join_field in on_fields:
                join_field_lower = join_field.lower()
                
                # Check dimension keywords
                for dim_type, keywords in dimension_keywords.items():
                    if any(kw in question_lower for kw in keywords):
                        if dim_type in join_field_lower:
                            score += 5
                            break
                
                # Check common fields
                common_fields = ['customer_id', 'cust_id', 'agent_id', 'mtn', 'recoverykey', 'call_id']
                if join_field_lower in common_fields:
                    score += 2
            
            # Bonus for having multiple join options
            if len(on_fields) > 1:
                score += 1
            
            # Prefer many_to_one relationships
            if join.get('join_type') == 'many_to_one':
                score += 1
            
            scored_joins.append({
                **join,
                'priority_score': score
            })
        
        return sorted(scored_joins, key=lambda x: (-x['priority_score'], str(x['on_field'])))
    
    def _execute_cypher(self, cypher_query: str) -> Dict[str, Any]:
        """Execute Cypher query against Neo4j"""
        
        with self.driver.session() as session:
            try:
                result = session.run(cypher_query)
                
                tables_dict = {}  # Use dict to deduplicate tables
                joins_dict = {}  # Use dict to consolidate joins
                
                for record in result:
                    table_name = record.get("table_name")
                    
                    # Deduplicate tables and merge columns
                    if table_name not in tables_dict:
                        # First time seeing this table - add it
                        tables_dict[table_name] = {
                            "name": table_name,
                            "type": record.get("table_type", "table"),
                            "business_description": record.get("table_description", ""),
                            "columns": record.get("columns_list", [])
                        }
                    else:
                        # Table already exists - merge columns (avoid duplicates)
                        existing_cols = {col['name']: col for col in tables_dict[table_name]['columns']}
                        
                        for new_col in record.get("columns_list", []):
                            if new_col['name'] not in existing_cols:
                                tables_dict[table_name]['columns'].append(new_col)
                    
                    # Process joins_list (array of joins)
                    joins_list = record.get("joins_list", [])
                    if joins_list:
                        for join in joins_list:
                            # Skip if join is None or missing required fields
                            if not join or not join.get("to_table") or not join.get("via_field"):
                                continue
                            
                            # Normalize via_field to always be a list
                            via_field = join["via_field"]
                            if not isinstance(via_field, list):
                                via_field = [via_field]
                            
                            # Create join key based on table pair only (not field)
                            from_table = record.get("table_name")
                            to_table = join["to_table"]
                            join_key = f"{from_table}-{to_table}"
                            
                            # Consolidate multiple fields for same table pair
                            if join_key in joins_dict:
                                # Add new fields to existing join
                                existing_fields = joins_dict[join_key]["on_field"]
                                for field in via_field:
                                    if field not in existing_fields:
                                        existing_fields.append(field)
                            else:
                                # Create new join entry with fields as list
                                joins_dict[join_key] = {
                                    "from_table": from_table,
                                    "to_table": to_table,
                                    "on_field": via_field.copy(),  # Keep as list
                                    "join_type": join.get("relationship_type", "many_to_one")
                                }
                
                # Convert dictionaries back to lists
                tables = list(tables_dict.values())
                joins = list(joins_dict.values())
                
                print(f"✅ Retrieved {len(tables)} relevant tables")
                if joins:
                    print(f"🔗 Found {len(joins)} join relationships")
                    # Print consolidated join info
                    for join in joins:
                        if len(join["on_field"]) > 1:
                            print(f"   📎 {join['from_table']} → {join['to_table']} via {join['on_field']}")
                
                return {
                    "tables": tables,
                    "joins": joins
                }
                
            except Exception as e:
                print(f"❌ Error executing Cypher: {e}"
