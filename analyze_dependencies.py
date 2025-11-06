#!/usr/bin/env python3
"""
MetaLayer DAG Dependency Analysis
Analyzes all DAG dependencies and task relationships
"""

import os
import re
from pathlib import Path


def analyze_dag_dependencies():
    """Analyze DAG dependencies and relationships"""

    dags_dir = Path("dags")
    dependency_map = {}
    dag_list = []

    # Find all DAG files
    for dag_file in dags_dir.glob("*.py"):
        if dag_file.name.startswith("__"):
            continue

        try:
            with open(dag_file, "r", encoding="utf-8") as f:
                content = f.read()

            # Extract DAG ID
            dag_id_match = re.search(r'dag_id\s*=\s*["\']([^"\']+)["\']', content)
            if dag_id_match:
                dag_id = dag_id_match.group(1)
                dag_list.append(dag_id)

                # Find external dependencies
                external_deps = []
                trigger_deps = []

                # Find ExternalTaskSensor dependencies
                external_matches = re.findall(
                    r'external_dag_id\s*=\s*["\']([^"\']+)["\']', content
                )
                external_deps.extend(external_matches)

                # Find TriggerDagRunOperator dependencies
                trigger_matches = re.findall(
                    r'trigger_dag_id\s*=\s*["\']([^"\']+)["\']', content
                )
                trigger_deps.extend(trigger_matches)

                # Find task dependencies within DAG
                task_deps = re.findall(r"(\w+)\s*>>\s*(\w+)", content)

                dependency_map[dag_id] = {
                    "file": dag_file.name,
                    "external_dependencies": external_deps,
                    "triggers": trigger_deps,
                    "internal_task_deps": task_deps,
                }

        except Exception as e:
            print(f"❌ Error analyzing {dag_file.name}: {e}")

    return dependency_map, dag_list


def validate_dependencies(dependency_map, dag_list):
    """Validate that all dependencies exist"""

    print("🔍 DEPENDENCY VALIDATION REPORT")
    print("=" * 50)

    issues = []

    for dag_id, info in dependency_map.items():
        print(f"\n📋 {dag_id} ({info['file']})")

        # Check external dependencies
        if info["external_dependencies"]:
            print(f"  📥 Waits for: {', '.join(info['external_dependencies'])}")
            for dep in info["external_dependencies"]:
                if dep not in dag_list:
                    issues.append(f"❌ {dag_id} depends on missing DAG: {dep}")
                else:
                    print(f"    ✅ {dep} - Found")

        # Check trigger dependencies
        if info["triggers"]:
            print(f"  📤 Triggers: {', '.join(info['triggers'])}")
            for dep in info["triggers"]:
                if dep not in dag_list:
                    issues.append(f"❌ {dag_id} triggers missing DAG: {dep}")
                else:
                    print(f"    ✅ {dep} - Found")

        # Show internal task dependencies
        if info["internal_task_deps"]:
            print(
                f"  🔗 Task flow: {' >> '.join(set([t[0] for t in info['internal_task_deps']] + [t[1] for t in info['internal_task_deps']]))}"
            )

    return issues


def analyze_medallion_architecture():
    """Analyze the medallion architecture flow"""

    print(f"\n🏗️ MEDALLION ARCHITECTURE ANALYSIS")
    print("=" * 50)

    # Expected medallion flow
    expected_flow = [
        "init_db_schemas_pg",
        "bronze_layer_production_load",
        "silver_layer_production_transform",
        "gold_layer_production_analytics",
    ]

    # Check if master orchestrator exists
    master_orchestrator = "medallion_master_orchestrator"

    print(f"Expected Flow: {' -> '.join(expected_flow)}")
    print(f"Master Orchestrator: {master_orchestrator}")

    return expected_flow


def main():
    """Run dependency analysis"""

    print("🚀 MetaLayer DAG Dependency Analysis")
    print("=" * 50)

    # Analyze dependencies
    dependency_map, dag_list = analyze_dag_dependencies()

    print(f"\n📊 FOUND {len(dag_list)} DAGS:")
    for dag in sorted(dag_list):
        print(f"  ✅ {dag}")

    # Validate dependencies
    issues = validate_dependencies(dependency_map, dag_list)

    # Analyze medallion architecture
    expected_flow = analyze_medallion_architecture()

    # Summary
    print(f"\n📋 DEPENDENCY SUMMARY")
    print("=" * 50)

    if issues:
        print("❌ ISSUES FOUND:")
        for issue in issues:
            print(f"  {issue}")
    else:
        print("✅ All dependencies are valid!")

    # Check medallion completeness
    medallion_complete = all(dag in dag_list for dag in expected_flow)
    if medallion_complete:
        print("✅ Complete medallion architecture found!")
    else:
        missing = [dag for dag in expected_flow if dag not in dag_list]
        print(f"⚠️  Missing medallion DAGs: {missing}")

    return len(issues) == 0 and medallion_complete


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
