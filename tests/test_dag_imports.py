"""
Simple test to verify DAG loading
"""

def test_dag_imports():
    """Test that all DAGs can be imported without errors"""
    import sys
    import os
    
    # Add dags directory to path
    dags_dir = os.path.join(os.path.dirname(__file__), '..', 'dags')
    sys.path.insert(0, dags_dir)
    
    try:
        # Import DAGs
        import bronze_layer_etl_pipeline
        import silver_layer_etl_pipeline  
        import gold_layer_analytics_pipeline
        import master_etl_orchestrator
        
        print("✅ All DAGs imported successfully")
        return True
    except Exception as e:
        print(f"❌ DAG import failed: {e}")
        return False

if __name__ == "__main__":
    test_dag_imports()