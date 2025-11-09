"""
Test configuration for pytest
"""
import sys
from pathlib import Path

# Add project directories to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "dags"))
sys.path.insert(0, str(project_root / "include"))
sys.path.insert(0, str(project_root / "plugins"))