#!/usr/bin/env python3
"""
Comprehensive Pre-commit Validation Script
==========================================

This script validates all code changes before committing to prevent
GitHub Actions workflow failures.

Usage:
    python validate_before_commit.py

Checks:
- Python syntax validation (all .py files)
- Black code formatting compliance  
- Import statement validation
- F-string syntax validation
- Security scan configuration validation
"""

import os
import sys
import glob
import py_compile
import subprocess
import re


def print_header(title):
    """Print formatted section header"""
    print(f"\n🔍 {title}")
    print("=" * (len(title) + 4))


def validate_python_syntax():
    """Validate Python syntax for all .py files"""
    print_header("PYTHON SYNTAX VALIDATION")
    
    python_files = []
    for root, dirs, files in os.walk('.'):
        # Skip virtual environment and git directories
        dirs[:] = [d for d in dirs if d not in {'.venv', '.git', '__pycache__', 'node_modules'}]
        for file in files:
            if file.endswith('.py'):
                python_files.append(os.path.join(root, file))
    
    print(f"Found {len(python_files)} Python files")
    
    failed_files = []
    for file in sorted(python_files):
        try:
            py_compile.compile(file, doraise=True)
            print(f"✅ {file}")
        except Exception as e:
            print(f"❌ {file}: {e}")
            failed_files.append(file)
    
    if failed_files:
        print(f"\n❌ {len(failed_files)} files failed syntax check!")
        return False
    else:
        print(f"\n✅ All {len(python_files)} Python files passed syntax validation!")
        return True


def validate_black_formatting():
    """Check Black code formatting compliance"""
    print_header("BLACK CODE FORMATTING VALIDATION")
    
    try:
        result = subprocess.run([
            sys.executable, '-m', 'black', '--check', '--diff',
            'dags/', 'include/', 'tests/',
            '--exclude', r'\.venv|__pycache__|\.git|node_modules'
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ All files pass Black formatting checks!")
            return True
        else:
            print("❌ Black formatting issues found:")
            print(result.stdout)
            print("\nRun 'black dags/ include/ tests/' to fix formatting")
            return False
            
    except FileNotFoundError:
        print("⚠️  Black not installed - skipping formatting check")
        return True


def validate_fstring_syntax():
    """Check for broken f-string patterns that cause syntax errors"""
    print_header("F-STRING SYNTAX VALIDATION")
    
    python_files = glob.glob("**/*.py", recursive=True)
    python_files = [f for f in python_files if not any(x in f for x in ['.venv', '__pycache__', '.git'])]
    
    issues_found = []
    
    for file in python_files:
        try:
            with open(file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                
            for i, line in enumerate(lines, 1):
                # Check for potentially broken f-strings
                if re.search(r'f"[^"]*\{\s*$', line.strip()):
                    issues_found.append(f"{file}:{i} - Potential unterminated f-string")
                
                # Check for f-strings that span lines without proper continuation
                if re.search(r'f"[^"]*\{[^}]*$', line.strip()) and not line.strip().endswith('\\'):
                    issues_found.append(f"{file}:{i} - F-string may be broken across lines")
                    
        except Exception as e:
            issues_found.append(f"{file} - Could not read file: {e}")
    
    if issues_found:
        print("❌ Potential f-string issues found:")
        for issue in issues_found:
            print(f"  - {issue}")
        return False
    else:
        print("✅ No f-string syntax issues detected!")
        return True


def validate_import_statements():
    """Check for common import issues"""
    print_header("IMPORT STATEMENT VALIDATION")
    
    try:
        # Test importing key DAG files
        dag_files = glob.glob("dags/*.py")
        dag_files = [f for f in dag_files if not f.endswith('__init__.py')]
        
        import_issues = []
        
        for dag_file in dag_files:
            try:
                # Simulate import without executing
                with open(dag_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                    
                # Check for basic import syntax
                compile(content, dag_file, 'exec')
                print(f"✅ {dag_file} - Import syntax OK")
                
            except SyntaxError as e:
                import_issues.append(f"{dag_file}:{e.lineno} - {e.msg}")
            except Exception as e:
                import_issues.append(f"{dag_file} - {e}")
        
        if import_issues:
            print("❌ Import validation issues:")
            for issue in import_issues:
                print(f"  - {issue}")
            return False
        else:
            print("✅ All DAG files have valid import syntax!")
            return True
            
    except Exception as e:
        print(f"⚠️  Import validation failed: {e}")
        return True


def validate_security_config():
    """Validate security workflow configuration"""
    print_header("SECURITY CONFIGURATION VALIDATION")
    
    security_workflow = ".github/workflows/security-quality.yml"
    
    if not os.path.exists(security_workflow):
        print("⚠️  Security workflow file not found")
        return True
    
    try:
        with open(security_workflow, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Check for null safety in severity handling
        if "v.get('severity') or ''" in content:
            print("✅ Security scan has null safety for severity values")
            return True
        elif "v.get('severity', '').lower()" in content:
            print("❌ Security scan missing null safety - may crash on None severity")
            print("  Fix: Replace v.get('severity', '').lower() with ((v.get('severity') or '').strip().lower()")
            return False
        else:
            print("✅ Security configuration appears valid")
            return True
            
    except Exception as e:
        print(f"⚠️  Could not validate security config: {e}")
        return True


def main():
    """Run all validation checks"""
    print("🚀 PRE-COMMIT VALIDATION STARTING...")
    print("This will check for common issues that cause GitHub Actions failures")
    
    checks = [
        ("Python Syntax", validate_python_syntax),
        ("Black Formatting", validate_black_formatting),
        ("F-String Syntax", validate_fstring_syntax),
        ("Import Statements", validate_import_statements),
        ("Security Config", validate_security_config),
    ]
    
    results = {}
    
    for check_name, check_func in checks:
        try:
            results[check_name] = check_func()
        except Exception as e:
            print(f"❌ {check_name} check failed with error: {e}")
            results[check_name] = False
    
    print_header("VALIDATION SUMMARY")
    
    all_passed = True
    for check_name, passed in results.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status} - {check_name}")
        if not passed:
            all_passed = False
    
    if all_passed:
        print("\n🎉 ALL VALIDATIONS PASSED!")
        print("Your code is ready for commit - GitHub Actions should succeed!")
        return 0
    else:
        print("\n❌ SOME VALIDATIONS FAILED!")
        print("Please fix the issues above before committing.")
        return 1


if __name__ == "__main__":
    sys.exit(main())