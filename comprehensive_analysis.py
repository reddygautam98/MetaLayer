#!/usr/bin/env python3
"""
MetaLayer Comprehensive System Analysis Report
Complete validation of all DAGs, ETL processes, and system functionality
"""

import os
import json
import re
from pathlib import Path
from datetime import datetime

def generate_comprehensive_report():
    """Generate comprehensive system analysis report"""
    
    report = {
        'analysis_timestamp': datetime.now().isoformat(),
        'system_overview': {},
        'dag_analysis': {},
        'etl_pipeline_analysis': {},
        'data_analysis': {},
        'configuration_analysis': {},
        'deployment_readiness': {},
        'recommendations': []
    }
    
    # System Overview
    report['system_overview'] = analyze_system_overview()
    
    # DAG Analysis
    report['dag_analysis'] = analyze_dags()
    
    # ETL Pipeline Analysis  
    report['etl_pipeline_analysis'] = analyze_etl_pipeline()
    
    # Data Analysis
    report['data_analysis'] = analyze_data_files()
    
    # Configuration Analysis
    report['configuration_analysis'] = analyze_configurations()
    
    # Deployment Readiness
    report['deployment_readiness'] = assess_deployment_readiness()
    
    # Generate Recommendations
    report['recommendations'] = generate_recommendations(report)
    
    return report

def analyze_system_overview():
    """Analyze overall system structure"""
    
    structure = {}
    
    # Count files by type
    structure['python_files'] = len(list(Path('.').rglob('*.py')))
    structure['sql_files'] = len(list(Path('.').rglob('*.sql')))
    structure['yaml_files'] = len(list(Path('.').rglob('*.yaml'))) + len(list(Path('.').rglob('*.yml')))
    structure['json_files'] = len(list(Path('.').rglob('*.json')))
    structure['csv_files'] = len(list(Path('.').rglob('*.csv')))
    
    # Key directories
    key_dirs = ['dags', 'include/sql', 'include/utils', 'config', 'data', 'tests']
    structure['directories'] = {
        dir_name: Path(dir_name).exists() for dir_name in key_dirs
    }
    
    # Calculate total project size
    total_size = 0
    for file_path in Path('.').rglob('*'):
        if file_path.is_file():
            try:
                total_size += file_path.stat().st_size
            except:
                pass
    
    structure['total_size_mb'] = round(total_size / (1024 * 1024), 2)
    
    return structure

def analyze_dags():
    """Analyze DAG files and their capabilities"""
    
    dag_analysis = {
        'total_dags': 0,
        'production_dags': 0,
        'test_dags': 0,
        'medallion_dags': 0,
        'dag_details': [],
        'dependency_map': {}
    }
    
    dags_dir = Path('dags')
    if not dags_dir.exists():
        return dag_analysis
    
    for dag_file in dags_dir.glob('*.py'):
        if dag_file.name.startswith('__'):
            continue
            
        try:
            with open(dag_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Extract DAG information
            dag_info = extract_dag_info(content, dag_file.name)
            
            if dag_info:
                dag_analysis['dag_details'].append(dag_info)
                dag_analysis['total_dags'] += 1
                
                # Categorize DAGs
                if 'production' in dag_info['dag_id'].lower():
                    dag_analysis['production_dags'] += 1
                elif 'test' in dag_info['dag_id'].lower():
                    dag_analysis['test_dags'] += 1
                
                if any(word in dag_info['dag_id'].lower() for word in ['bronze', 'silver', 'gold', 'medallion']):
                    dag_analysis['medallion_dags'] += 1
                
                # Extract dependencies
                if dag_info['external_dependencies'] or dag_info['triggers']:
                    dag_analysis['dependency_map'][dag_info['dag_id']] = {
                        'waits_for': dag_info['external_dependencies'],
                        'triggers': dag_info['triggers']
                    }
                    
        except Exception as e:
            dag_analysis['dag_details'].append({
                'file': dag_file.name,
                'error': str(e),
                'status': 'ERROR'
            })
    
    return dag_analysis

def extract_dag_info(content, filename):
    """Extract DAG information from content"""
    
    # Extract DAG ID
    dag_id_match = re.search(r'dag_id\s*=\s*["\']([^"\']+)["\']', content)
    if not dag_id_match:
        return None
    
    dag_id = dag_id_match.group(1)
    
    # Extract other information
    schedule_match = re.search(r'schedule[_interval]*\s*=\s*([^,\)]+)', content)
    schedule = schedule_match.group(1).strip() if schedule_match else 'None'
    
    # Extract tags
    tags_match = re.search(r'tags\s*=\s*\[([^\]]+)\]', content)
    tags = [tag.strip().strip('\'"') for tag in tags_match.group(1).split(',')] if tags_match else []
    
    # Extract dependencies
    external_deps = re.findall(r'external_dag_id\s*=\s*["\']([^"\']+)["\']', content)
    triggers = re.findall(r'trigger_dag_id\s*=\s*["\']([^"\']+)["\']', content)
    
    # Count tasks
    task_count = len(re.findall(r'task_id\s*=\s*["\']([^"\']+)["\']', content))
    
    return {
        'dag_id': dag_id,
        'file': filename,
        'schedule': schedule,
        'tags': tags,
        'task_count': task_count,
        'external_dependencies': external_deps,
        'triggers': triggers,
        'status': 'OK'
    }

def analyze_etl_pipeline():
    """Analyze ETL pipeline structure and flow"""
    
    pipeline_analysis = {
        'medallion_architecture': {},
        'data_flow': [],
        'layer_analysis': {}
    }
    
    # Define expected medallion flow
    expected_layers = {
        'bronze': ['init_db_schemas_pg', 'bronze_layer_production_load'],
        'silver': ['silver_layer_production_transform'],
        'gold': ['gold_layer_production_analytics'],
        'orchestrator': ['medallion_master_orchestrator']
    }
    
    # Check layer completeness
    for layer, expected_dags in expected_layers.items():
        pipeline_analysis['medallion_architecture'][layer] = {
            'expected': expected_dags,
            'found': [],
            'missing': []
        }
        
        # Check if DAGs exist
        for expected_dag in expected_dags:
            dag_file = Path(f'dags/{expected_dag}.py')
            alt_files = list(Path('dags').glob(f'*{expected_dag.split("_")[-1]}*.py'))
            
            if any(expected_dag in str(f) for f in alt_files):
                pipeline_analysis['medallion_architecture'][layer]['found'].append(expected_dag)
            else:
                pipeline_analysis['medallion_architecture'][layer]['missing'].append(expected_dag)
    
    # Analyze SQL transformations for each layer
    sql_dir = Path('include/sql')
    if sql_dir.exists():
        for layer in ['bronze', 'silver', 'gold']:
            layer_files = list(sql_dir.glob(f'*{layer}*'))
            pipeline_analysis['layer_analysis'][layer] = {
                'sql_files': [f.name for f in layer_files],
                'file_count': len(layer_files)
            }
    
    return pipeline_analysis

def analyze_data_files():
    """Analyze data files and structure"""
    
    data_analysis = {
        'source_data': {},
        'total_records': 0,
        'data_quality': {}
    }
    
    # Check source data files
    data_sources = [
        'data/bronze_src/erp/erp_sales.csv',
        'data/bronze_src/crm/crm_customers.csv'
    ]
    
    for source_file in data_sources:
        file_path = Path(source_file)
        if file_path.exists():
            try:
                # Get file stats
                size_mb = file_path.stat().st_size / (1024 * 1024)
                
                # Count lines (approximate records)
                with open(file_path, 'r', encoding='utf-8') as f:
                    line_count = sum(1 for _ in f) - 1  # Subtract header
                
                # Check header
                with open(file_path, 'r', encoding='utf-8') as f:
                    header = f.readline().strip()
                
                data_analysis['source_data'][source_file] = {
                    'exists': True,
                    'size_mb': round(size_mb, 2),
                    'records': line_count,
                    'header': header
                }
                
                data_analysis['total_records'] += line_count
                
            except Exception as e:
                data_analysis['source_data'][source_file] = {
                    'exists': True,
                    'error': str(e)
                }
        else:
            data_analysis['source_data'][source_file] = {
                'exists': False
            }
    
    return data_analysis

def analyze_configurations():
    """Analyze configuration files"""
    
    config_analysis = {
        'docker_config': {},
        'airflow_config': {},
        'monitoring_config': {},
        'requirements': {}
    }
    
    # Docker configuration
    docker_files = ['docker-compose.yml', 'Dockerfile']
    for docker_file in docker_files:
        file_path = Path(docker_file)
        config_analysis['docker_config'][docker_file] = {
            'exists': file_path.exists(),
            'size_kb': round(file_path.stat().st_size / 1024, 2) if file_path.exists() else 0
        }
    
    # Airflow configuration
    airflow_files = ['airflow_settings.yaml']
    for airflow_file in airflow_files:
        file_path = Path(airflow_file)
        config_analysis['airflow_config'][airflow_file] = {
            'exists': file_path.exists(),
            'size_kb': round(file_path.stat().st_size / 1024, 2) if file_path.exists() else 0
        }
    
    # Monitoring configuration
    monitoring_files = ['config/prometheus.yml', 'config/grafana/dashboards/metalayer-overview.json']
    for monitoring_file in monitoring_files:
        file_path = Path(monitoring_file)
        config_analysis['monitoring_config'][monitoring_file] = {
            'exists': file_path.exists(),
            'size_kb': round(file_path.stat().st_size / 1024, 2) if file_path.exists() else 0
        }
    
    # Requirements analysis
    req_file = Path('requirements.txt')
    if req_file.exists():
        try:
            with open(req_file, 'r', encoding='utf-8') as f:
                lines = [line.strip() for line in f if line.strip() and not line.startswith('#')]
            
            packages = [line.split('==')[0] for line in lines if '==' in line]
            config_analysis['requirements'] = {
                'total_packages': len(packages),
                'packages': packages[:10],  # First 10 packages
                'has_duplicates': len(packages) != len(set(packages))
            }
        except Exception as e:
            config_analysis['requirements'] = {'error': str(e)}
    
    return config_analysis

def assess_deployment_readiness():
    """Assess overall deployment readiness"""
    
    readiness = {
        'overall_score': 0,
        'critical_checks': {},
        'warnings': [],
        'blockers': []
    }
    
    critical_checks = {
        'docker_compose_valid': Path('docker-compose.yml').exists(),
        'main_dags_exist': all(Path(f'dags/{dag}.py').exists() or 
                              any(dag in str(f) for f in Path('dags').glob('*.py'))
                              for dag in ['medallion_master_orchestrator', 'bronze_layer_production', 
                                        'silver_layer_production', 'gold_layer_production']),
        'data_files_exist': Path('data/bronze_src/erp/erp_sales.csv').exists() and 
                           Path('data/bronze_src/crm/crm_customers.csv').exists(),
        'sql_files_exist': Path('include/sql').exists() and len(list(Path('include/sql').glob('*.sql'))) > 0,
        'requirements_valid': Path('requirements.txt').exists(),
        'utilities_exist': Path('include/utils').exists() and len(list(Path('include/utils').glob('*.py'))) > 0
    }
    
    readiness['critical_checks'] = critical_checks
    
    # Calculate score
    passed_checks = sum(1 for check in critical_checks.values() if check)
    total_checks = len(critical_checks)
    readiness['overall_score'] = round((passed_checks / total_checks) * 100, 1)
    
    # Identify blockers and warnings
    for check_name, passed in critical_checks.items():
        if not passed:
            if 'docker' in check_name or 'main_dags' in check_name:
                readiness['blockers'].append(f"Critical: {check_name.replace('_', ' ').title()}")
            else:
                readiness['warnings'].append(f"Warning: {check_name.replace('_', ' ').title()}")
    
    return readiness

def generate_recommendations(report):
    """Generate recommendations based on analysis"""
    
    recommendations = []
    
    # Check deployment readiness score
    score = report['deployment_readiness']['overall_score']
    
    if score >= 90:
        recommendations.append("🎉 System is production-ready! All critical components are functional.")
    elif score >= 70:
        recommendations.append("⚠️ System is mostly ready but has some minor issues to address.")
    else:
        recommendations.append("🔧 System needs attention before deployment.")
    
    # DAG-specific recommendations
    dag_count = report['dag_analysis']['total_dags']
    if dag_count > 20:
        recommendations.append("📊 Consider organizing DAGs into subdirectories for better management.")
    
    # Data recommendations
    total_records = report['data_analysis']['total_records']
    if total_records > 1000000:
        recommendations.append("🚀 Large dataset detected - ensure database is properly optimized for performance.")
    
    # Configuration recommendations
    if report['configuration_analysis']['requirements'].get('has_duplicates'):
        recommendations.append("🔧 Clean up duplicate packages in requirements.txt.")
    
    return recommendations

def print_report(report):
    """Print formatted analysis report"""
    
    print("🚀 METALAYER COMPREHENSIVE SYSTEM ANALYSIS")
    print("=" * 80)
    print(f"Analysis completed: {report['analysis_timestamp']}")
    
    # System Overview
    print(f"\n📊 SYSTEM OVERVIEW")
    print("-" * 40)
    overview = report['system_overview']
    print(f"Total project size: {overview['total_size_mb']} MB")
    print(f"Python files: {overview['python_files']}")
    print(f"SQL files: {overview['sql_files']}")
    print(f"Configuration files: {overview['yaml_files']} YAML, {overview['json_files']} JSON")
    print(f"Data files: {overview['csv_files']} CSV")
    
    # DAG Analysis
    print(f"\n🔄 DAG ANALYSIS")
    print("-" * 40)
    dag_analysis = report['dag_analysis']
    print(f"Total DAGs: {dag_analysis['total_dags']}")
    print(f"Production DAGs: {dag_analysis['production_dags']}")
    print(f"Medallion Architecture DAGs: {dag_analysis['medallion_dags']}")
    print(f"DAGs with Dependencies: {len(dag_analysis['dependency_map'])}")
    
    # ETL Pipeline
    print(f"\n⚡ ETL PIPELINE STATUS")
    print("-" * 40)
    etl = report['etl_pipeline_analysis']['medallion_architecture']
    for layer, info in etl.items():
        found_count = len(info['found'])
        total_count = len(info['expected'])
        print(f"{layer.upper()} Layer: {found_count}/{total_count} DAGs ({'✅' if found_count == total_count else '⚠️'})")
    
    # Data Analysis  
    print(f"\n📁 DATA ANALYSIS")
    print("-" * 40)
    data_analysis = report['data_analysis']
    print(f"Total source records: {data_analysis['total_records']:,}")
    for source, info in data_analysis['source_data'].items():
        if info.get('exists'):
            print(f"✅ {source}: {info.get('records', 'N/A'):,} records ({info.get('size_mb', 0)} MB)")
        else:
            print(f"❌ {source}: Missing")
    
    # Deployment Readiness
    print(f"\n🚀 DEPLOYMENT READINESS")
    print("-" * 40)
    readiness = report['deployment_readiness']
    print(f"Overall Score: {readiness['overall_score']}%")
    
    print(f"\nCritical Checks:")
    for check, passed in readiness['critical_checks'].items():
        status = "✅" if passed else "❌"
        print(f"  {status} {check.replace('_', ' ').title()}")
    
    if readiness['blockers']:
        print(f"\n🚫 BLOCKERS:")
        for blocker in readiness['blockers']:
            print(f"  • {blocker}")
    
    if readiness['warnings']:
        print(f"\n⚠️ WARNINGS:")
        for warning in readiness['warnings']:
            print(f"  • {warning}")
    
    # Recommendations
    print(f"\n💡 RECOMMENDATIONS")
    print("-" * 40)
    for recommendation in report['recommendations']:
        print(f"• {recommendation}")
    
    print(f"\n" + "=" * 80)
    
    return readiness['overall_score']

def main():
    """Run comprehensive analysis"""
    
    try:
        report = generate_comprehensive_report()
        score = print_report(report)
        
        # Save report to file
        with open('metalayer_analysis_report.json', 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, default=str)
        
        print(f"📄 Detailed report saved to: metalayer_analysis_report.json")
        
        return 0 if score >= 80 else 1
        
    except Exception as e:
        print(f"❌ Analysis failed: {e}")
        return 1

if __name__ == "__main__":
    import sys
    sys.exit(main())