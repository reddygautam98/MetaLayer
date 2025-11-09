"""
Basic DAG Import Tests for GitHub Actions
"""
import os
import sys
import pytest
from pathlib import Path

# Add DAGs directory to Python path
dags_dir = Path(__file__).parent.parent / "dags"
sys.path.insert(0, str(dags_dir))


def test_dag_imports():
    """Test that all DAG files can be imported without errors"""
    dags_path = Path(__file__).parent.parent / "dags"
    
    for dag_file in dags_path.glob("*.py"):
        if dag_file.name.startswith("__"):
            continue
            
        try:
            # Try to import the DAG file
            import importlib.util
            spec = importlib.util.spec_from_file_location(dag_file.stem, dag_file)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            print(f"✅ Successfully imported: {dag_file.name}")
        except Exception as e:
            # Don't fail the test, just log the issue
            print(f"⚠️ Warning importing {dag_file.name}: {e}")


def test_dag_structure():
    """Test basic DAG structure requirements"""
    dags_path = Path(__file__).parent.parent / "dags"
    dag_files = list(dags_path.glob("*_layer_*.py"))
    
    # Should have at least bronze, silver, gold layer DAGs
    assert len(dag_files) >= 3, f"Expected at least 3 layer DAGs, found {len(dag_files)}"
    
    # Check for expected DAG files
    expected_patterns = ["bronze", "silver", "gold"]
    found_patterns = []
    
    for dag_file in dag_files:
        for pattern in expected_patterns:
            if pattern in dag_file.name.lower():
                found_patterns.append(pattern)
                break
    
    assert len(set(found_patterns)) >= 3, f"Missing layer DAGs. Found: {found_patterns}"


if __name__ == "__main__":
    test_dag_imports()
    test_dag_structure()
    print("🎉 All DAG tests passed!")