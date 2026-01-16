"""
Unified Ontology Loader
=======================

This script loads a complete knowledge graph into Neo4j in three layers:

LAYER 1: Lower Ontology (Physical Schema)
- Tables: Physical database tables with descriptions
- Columns: Table columns with data types and properties
- Relationships: Foreign key relationships between tables

LAYER 2: Business Context (Semantic Enrichment)
- Enriches columns with business terminology
- Adds semantic types, definitions, and usage notes
- Provides sample values and data quality information

LAYER 3: Upper Ontology (Business Concepts)
- Business concepts that span multiple tables
- Maps concepts to relevant tables with confidence scores
- Enables concept-first metadata retrieval

Execution Order (all in one script):
1. Clear existing graph (optional)
2. Load physical schema (tables, columns, relationships)
3. Enrich columns with business context
4. Create concept ontology and mappings

Usage:
    python unified_ontology_loader.py

Prerequisites:
- Neo4j database running
- Environment variables: NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD
- JSON files: ontology.json, business_context.json, business_concepts.json
"""

from neo4j import GraphDatabase
import json
from typing import Dict, Any, List
import os
from dotenv import load_dotenv

load_dotenv()


class UnifiedOntologyLoader:
    def __init__(self, uri: str, username: str, password: str):
        self.driver = GraphDatabase.driver(uri, auth=(username, password))
        
    def close(self):
        self.driver.close()
    
    # =========================================================================
    # LAYER 0: Graph Initialization
    # =========================================================================
    
    def clear_metadata_graph(self):
        """Clear existing metadata graph"""
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
            print("🗑️  Cleared existing metadata graph")
    
    # =========================================================================
    # LAYER 1: Load Physical Schema (Lower Ontology)
    # =========================================================================
    
    def load_physical_schema(self, ontology_file: str = "ontology.json"):
        """
        Load physical database schema into Neo4j
        
        Creates:
        - (:Table) nodes with properties
        - (:Column) nodes with properties
        - (:Table)-[:HAS_COLUMN]->(:Column)
        - (:Column)-[:FOREIGN_KEY_TO]->(:Column)
        - (:Table)-[:JOINS_WITH]->(:Table)
        """
        print("\n" + "="*70)
        print("📥 LAYER 1: Loading Physical Schema (Lower Ontology)")
        print("="*70)
        
        # Load ontology file
        try:
            with open(ontology_file, 'r') as f:
                ontology = json.load(f)
        except FileNotFoundError:
            print(f"❌ Error: {ontology_file} not found!")
            return False
        except json.JSONDecodeError as e:
            print(f"❌ Error: Invalid JSON in {ontology_file}: {e}")
            return False
        
        with self.driver.session() as session:
            # Create Table nodes
            tables = ontology.get("tables", [])
            print(f"\n📊 Creating {len(tables)} Table nodes...")
            
            for table in tables:
                session.run("""
                    CREATE (t:Table {
                        name: $name,
                        type: $type,
                        business_description: $description,
                        primary_key: $primary_key
                    })
                """, 
                    name=table["name"],
                    type=table["type"],
                    description=table.get("business_description", ""),
                    primary_key=table.get("primary_key", "")
                )
                print(f"  ✓ Created Table: {table['name']}")
                
                # Create Column nodes and relationships
                for column in table.get("columns", []):
                    session.run("""
                        MATCH (t:Table {name: $table_name})
                        CREATE (c:Column {
                            name: $col_name,
                            full_name: $full_name,
                            data_type: $data_type,
                            business_description: $description,
                            is_metric: $is_metric,
                            is_dimension: $is_dimension,
                            sample_values: $sample_values
                        })
                        CREATE (t)-[:HAS_COLUMN]->(c)
                    """,
                        table_name=table["name"],
                        col_name=column["name"],
                        full_name=f"{table['name']}.{column['name']}",
                        data_type=column.get("data_type", "unknown"),
                        description=column.get("business_description", ""),
                        is_metric=column.get("is_metric", False),
                        is_dimension=column.get("is_dimension", True),
                        sample_values=json.dumps(column.get("sample_values", []))
                    )
                
                print(f"  ✓ Created {len(table.get('columns', []))} columns")
            
            # Create relationship edges between tables
            relationships = ontology.get("relationships", [])
            print(f"\n🔗 Creating {len(relationships)} table relationships...")
            
            for relationship in relationships:
                session.run("""
                    MATCH (from_table:Table {name: $from_table})
                    MATCH (to_table:Table {name: $to_table})
                    MERGE (from_table)-[r:JOINS_WITH {
                        via_field: $via_field,
                        relationship_type: $rel_type,
                        business_description: $description
                    }]->(to_table)
                """,
                    from_table=relationship["from_table"],
                    to_table=relationship["to_table"],
                    via_field=relationship.get("via_field", ""),
                    rel_type=relationship.get("relationship_type", ""),
                    description=relationship.get("business_description", "")
                )
            
            print(f"  ✓ Created {len(relationships)} relationships")
            
            # Create foreign key relationships at column level
            for relationship in relationships:
                if relationship.get("via_field"):
                    session.run("""
                        MATCH (from_table:Table {name: $from_table})-[:HAS_COLUMN]->(fk_col:Column {name: $via_field})
                        MATCH (to_table:Table {name: $to_table})-[:HAS_COLUMN]->(pk_col:Column {name: $via_field})
                        MERGE (fk_col)-[:FOREIGN_KEY_TO]->(pk_col)
                    """,
                        from_table=relationship["from_table"],
                        to_table=relationship["to_table"],
                        via_field=relationship["via_field"]
                    )
            
            # Store key metrics
            metrics = ontology.get("key_metrics", [])
            if metrics:
                print(f"\n📈 Creating {len(metrics)} Metric nodes...")
                for metric in metrics:
                    session.run("""
                        CREATE (m:Metric {
                            name: $name,
                            description: $description,
                            calculation: $calculation,
                            tables_involved: $tables
                        })
                    """,
                        name=metric["name"],
                        description=metric.get("description", ""),
                        calculation=metric.get("calculation", ""),
                        tables=json.dumps(metric.get("tables_involved", []))
                    )
                print(f"  ✓ Created {len(metrics)} metrics")
            
            # Store common dimensions
            dimensions = ontology.get("common_dimensions", [])
            if dimensions:
                print(f"\n📐 Creating {len(dimensions)} Dimension nodes...")
                for dimension in dimensions:
                    session.run("""
                        CREATE (d:Dimension {
                            name: $name,
                            description: $description,
                            tables: $tables,
                            columns: $columns
                        })
                    """,
                        name=dimension["name"],
                        description=dimension.get("description", ""),
                        tables=json.dumps(dimension.get("tables", [])),
                        columns=json.dumps(dimension.get("columns", []))
                    )
                print(f"  ✓ Created {len(dimensions)} dimensions")
        
        print("\n✅ Physical schema loaded successfully!")
        return True
    
    # =========================================================================
    # LAYER 2: Enrich with Business Context
    # =========================================================================
    
    def enrich_business_context(self, context_file: str = "business_context.json"):
        """
        Enrich Column nodes with business context
        
        Adds properties:
        - business_term: Business-friendly name
        - business_definition: What the column represents
        - semantic_type: Type of data (identifier, metric, category, etc.)
        - usage_notes: How to use this column
        - data_quality_note: Known data quality issues
        - sample_values: Example values
        """
        print("\n" + "="*70)
        print("📥 LAYER 2: Enriching Business Context")
        print("="*70)
        
        # Load business context file
        try:
            with open(context_file, 'r') as f:
                data = json.load(f)
        except FileNotFoundError:
            print(f"⚠️  Warning: {context_file} not found, skipping business context")
            return True
        except json.JSONDecodeError as e:
            print(f"❌ Error: Invalid JSON in {context_file}: {e}")
            return False
        
        contexts = data.get("business_context", [])
        
        if not contexts:
            print("⚠️  No business context entries found")
            return True
        
        print(f"\n📊 Processing {len(contexts)} column enrichments...")
        
        # Track results
        success_count = 0
        not_found_count = 0
        
        with self.driver.session() as session:
            for context in contexts:
                table = context.get("table")
                column = context.get("column")
                
                if not table or not column:
                    continue
                
                # Check if column exists
                result = session.run("""
                    MATCH (t:Table {name: $table})-[:HAS_COLUMN]->(c:Column {name: $column})
                    RETURN c
                """, table=table, column=column)
                
                if not result.single():
                    not_found_count += 1
                    continue
                
                # Build SET clause dynamically
                set_clauses = []
                params = {"table": table, "column": column}
                
                field_mapping = {
                    "business_term": "business_term",
                    "business_definition": "business_definition",
                    "sample_values": "sample_values",
                    "semantic_type": "semantic_type",
                    "usage_notes": "usage_notes",
                    "unit": "unit",
                    "data_quality_note": "data_quality_note"
                }
                
                for json_field, neo4j_prop in field_mapping.items():
                    if json_field in context and context[json_field] is not None:
                        set_clauses.append(f"c.{neo4j_prop} = ${neo4j_prop}")
                        params[neo4j_prop] = context[json_field]
                
                if not set_clauses:
                    continue
                
                # Update the column
                cypher = f"""
                    MATCH (t:Table {{name: $table}})-[:HAS_COLUMN]->(c:Column {{name: $column}})
                    SET {', '.join(set_clauses)}
                """
                
                try:
                    session.run(cypher, **params)
                    success_count += 1
                except Exception as e:
                    print(f"  ❌ Error updating {table}.{column}: {e}")
        
        print(f"\n✅ Business context enrichment complete:")
        print(f"  • Successfully updated: {success_count}")
        print(f"  • Not found: {not_found_count}")
        
        return True
    
    # =========================================================================
    # LAYER 3: Create Concept Ontology (Upper Ontology)
    # =========================================================================
    
    def create_concept_ontology(self, concepts_file: str = "business_concepts.json"):
        """
        Create business concept layer (upper ontology)
        
        Creates:
        - (:Concept) nodes with business definitions
        - (:Concept)-[:RELATES_TO {confidence}]->(:Table)
        
        Concepts represent high-level business ideas that span multiple tables.
        Example: "Customer Sentiment" concept relates to sentiment tables and columns.
        """
        print("\n" + "="*70)
        print("📥 LAYER 3: Creating Concept Ontology (Upper Ontology)")
        print("="*70)
        
        # Load concepts file
        try:
            with open(concepts_file, 'r') as f:
                concepts = json.load(f)
        except FileNotFoundError:
            print(f"⚠️  Warning: {concepts_file} not found, skipping concept ontology")
            return True
        except json.JSONDecodeError as e:
            print(f"❌ Error: Invalid JSON in {concepts_file}: {e}")
            return False
        
        if not concepts:
            print("⚠️  No concepts found")
            return True
        
        print(f"\n📊 Processing {len(concepts)} business concepts...")
        
        # Track results
        concept_count = 0
        table_mapping_count = 0
        table_mapping_failures = 0
        
        with self.driver.session() as session:
            for concept_data in concepts:
                concept_name = concept_data.get("concept")
                definition = concept_data.get("definition", "")
                tables = concept_data.get("tables", [])
                
                if not concept_name:
                    continue
                
                # Create Concept node
                try:
                    session.run("""
                        MERGE (c:Concept {name: $name})
                        SET c.definition = $definition,
                            c.table_count = $table_count
                    """, 
                        name=concept_name,
                        definition=definition,
                        table_count=len(tables)
                    )
                    concept_count += 1
                    print(f"  ✓ Created concept: {concept_name}")
                except Exception as e:
                    print(f"  ❌ Error creating concept: {e}")
                    continue
                
                # Create RELATES_TO relationships to tables
                for table_mapping in tables:
                    table_name = table_mapping.get("name")
                    confidence = table_mapping.get("confidence", "medium")
                    
                    if not table_name:
                        table_mapping_failures += 1
                        continue
                    
                    result = session.run("""
                        MATCH (concept:Concept {name: $concept_name})
                        MATCH (t:Table {name: $table_name})
                        MERGE (concept)-[r:RELATES_TO]->(t)
                        SET r.confidence = $confidence
                        RETURN t.name as table_name
                    """,
                        concept_name=concept_name,
                        table_name=table_name,
                        confidence=confidence
                    )
                    
                    if result.single():
                        table_mapping_count += 1
                    else:
                        table_mapping_failures += 1
        
        print(f"\n✅ Concept ontology complete:")
        print(f"  • Concepts created: {concept_count}")
        print(f"  • Table mappings: {table_mapping_count}")
        print(f"  • Mapping failures: {table_mapping_failures}")
        
        return True
        
    # =========================================================================
    # Main Load Method
    # =========================================================================
    
    def load_complete_ontology(
        self,
        ontology_file: str = "ontology.json",
        context_file: str = "business_context.json",
        concepts_file: str = "business_concepts.json",
        clear_existing: bool = True
    ):
        """
        Load complete three-layer ontology in correct order
        
        Args:
            ontology_file: Physical schema JSON file
            context_file: Business context JSON file
            concepts_file: Business concepts JSON file
            clear_existing: Whether to clear existing graph first
        """
        print("\n" + "="*70)
        print("🏗️  UNIFIED ONTOLOGY LOADER")
        print("="*70)
        print("\nLoading three-layer knowledge graph:")
        print("  Layer 1: Physical Schema (Tables, Columns, Relationships)")
        print("  Layer 2: Business Context (Semantic Enrichment)")
        print("  Layer 3: Concept Ontology (Business Concepts)")
        
        # Step 0: Clear existing (optional)
        if clear_existing:
            print("\n" + "="*70)
            self.clear_metadata_graph()
        
        # Step 1: Load physical schema
        if not self.load_physical_schema(ontology_file):
            print("\n❌ Failed to load physical schema. Aborting.")
            return False
        
        # Step 2: Enrich with business context
        if not self.enrich_business_context(context_file):
            print("\n❌ Failed to enrich business context. Aborting.")
            return False
        
        # Step 3: Create concept ontology
        if not self.create_concept_ontology(concepts_file):
            print("\n❌ Failed to create concept ontology. Aborting.")
            return False
        
        # Verify complete ontology
        self.verify_complete_ontology()
        
        print("\n" + "="*70)
        print("✅ COMPLETE ONTOLOGY LOADED SUCCESSFULLY!")
        print("="*70)
        
        return True


if __name__ == "__main__":
    # Initialize loader
    loader = UnifiedOntologyLoader(
        uri=os.getenv("NEO4J_URI", "bolt://localhost:7687"),
        username=os.getenv("NEO4J_USERNAME", "neo4j"),
        password=os.getenv("NEO4J_PASSWORD", "password")
    )
    
    try:
        # Load complete ontology
        success = loader.load_complete_ontology(
            ontology_file="ontology.json",
            context_file="business_context.json",
            concepts_file="business_concepts.json",
            clear_existing=True
        )
        
        if success:
            print("\n🎉 Ontology is ready for use!")
        else:
            print("\n❌ Ontology loading failed!")
            
    finally:
        loader.close()
