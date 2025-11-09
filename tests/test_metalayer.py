"""
Basic tests for MetaLayer ETL Pipeline
"""
import os
import sys

def test_project_structure():
    """Test basic project structure exists"""
    assert os.path.exists('dags'), "DAGs directory should exist"
    assert os.path.exists('config'), "Config directory should exist"

def test_requirements_file():
    """Test requirements.txt exists and is readable"""
    assert os.path.exists('requirements.txt'), "requirements.txt should exist"
    
    with open('requirements.txt', 'r') as f:
        content = f.read()
        assert len(content) > 0, "requirements.txt should not be empty"

def test_python_version():
    """Test Python version compatibility"""
    assert sys.version_info >= (3, 8), "Python 3.8+ is required"

def test_dag_files_exist():
    """Test DAG files exist"""
    dag_files = []
    if os.path.exists('dags'):
        for file in os.listdir('dags'):
            if file.endswith('.py') and not file.startswith('__'):
                dag_files.append(file)
    
    assert len(dag_files) > 0, "At least one DAG file should exist"

if __name__ == "__main__":
    # Run basic validation when executed directly
    print("Running MetaLayer validation tests...")
    
    try:
        test_project_structure()
        print("✓ Project structure validation passed")
        
        test_requirements_file()
        print("✓ Requirements file validation passed")
        
        test_python_version()
        print("✓ Python version validation passed")
        
        test_dag_files_exist()
        print("✓ DAG files validation passed")
        
        print("\nAll validation tests passed successfully!")
        
    except AssertionError as e:
        print(f"✗ Validation failed: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        sys.exit(1)