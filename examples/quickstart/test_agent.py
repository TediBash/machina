#!/usr/bin/env python3
"""Your first maintenance agent — 13 lines of Python.

pip install machina-ai[litellm]
ollama pull llama3
python agent.py
"""

from __future__ import annotations

import sys
from pathlib import Path

_repo_root = Path(__file__).resolve().parent.parent.parent
_examples_dir = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_repo_root / "src"))
sys.path.insert(0, str(_examples_dir))

from _mode import add_mode_flags, resolve_sandbox  # noqa: E402
from _preflight import check  # noqa: E402

from machina import Agent, Plant
from machina.connectors.cmms import GenericCmmsConnector
from machina.connectors.comms.cli import CliChannel
from machina.connectors.docs import DocumentStoreConnector
from machina.connectors.docs import ExcelCsvConnector
from machina.connectors.sql import GenericSqlConnector
from machina.connectors.sql.schema import SqlConnectorConfig
from machina.connectors.iot.simulated import SimulatedSensorConnector
from machina.connectors.docs.excel_schema import ExcelConnectorConfig, SheetSchema, ColumnMapping


from machina.workflows import Workflow, Step, Trigger, TriggerType, ErrorPolicy


# Turn these to False to disable specific systems during testing
ENABLE_EXCEL_CMMS = True
ENABLE_DOCUMENTS = True
ENABLE_SENSORS = True
ENABLE_WORKFLOWS = True
ENABLE_SQL = True
ENABLE_CMMS = True


# --- 3. The Connector Factory ---
SAMPLE_DIR = _examples_dir / "sample_data"
active_connectors = []

if ENABLE_EXCEL_CMMS:
    excel_path = str(SAMPLE_DIR / "test.xlsx")
    
    # 1. Create the configuration object
    excel_config = ExcelConnectorConfig(
        asset_registry=SheetSchema(
            path=excel_path,         # Pointing to the same Excel file
            sheet="Assets",          # We will create a new tab named 'Assets'
            columns=[
                ColumnMapping(column="equip_id", field="id", required=True),
                ColumnMapping(column="equip_desc", field="name", required=True),
                ColumnMapping(column="equip_category", field="type"),
                ColumnMapping(column="risk_level", field="criticality"),
                ColumnMapping(column="iso_class", field="equipment_class_code"),
                ColumnMapping(column="parent_equip_id", field="parent_id")
            ]
        ),
        work_orders=SheetSchema(
            path=excel_path,
            sheet="Sheet1",
            write_mode="append",
            # 2. Map the Excel column headers to Machina's internal fields
            # The 'column' names must EXACTLY match the headers in your .xlsx file!
            columns=[
                ColumnMapping(column="ticket_num", field="id", required=True),
                ColumnMapping(column="target_equip_id", field="asset_id", required=True),
                ColumnMapping(column="job_type", field="type"),
                ColumnMapping(column="current_state", field="status"),
                ColumnMapping(column="urgency", field="priority"),
                ColumnMapping(column="issue_notes", field="description")
            ]
        )
    )

    # 3. Pass the config into the connector
    excel_connector = ExcelCsvConnector(config=excel_config)
    active_connectors.append(excel_connector)

if ENABLE_DOCUMENTS:
    doc_connector = DocumentStoreConnector(
        paths=[
            SAMPLE_DIR / "manuals"
        ]
    )
    active_connectors.append(doc_connector)

if ENABLE_CMMS:
    cmms_connector = GenericCmmsConnector(data_dir=SAMPLE_DIR / "cmms")
    active_connectors.append(cmms_connector)

if ENABLE_SENSORS:
    sensor_connector = SimulatedSensorConnector(
        data_dir=SAMPLE_DIR / "sensor_logs"
    )
    active_connectors.append(sensor_connector)

# --- 4. The Workflow Factory ---
active_workflows = []

if ENABLE_WORKFLOWS:
    # Workflow A: Predictive Catch (Data-Initiated)
    predictive_workflow = Workflow(
        name="Automated Alarm Response",
        description="Diagnose critical alarms and draft a work order.",
        trigger=Trigger(type=TriggerType.ALARM, filter={"severity": ["critical"]}),
        steps=[
            Step(name="diagnose", action="failure_analyzer.diagnose", on_error=ErrorPolicy.STOP),
            Step(name="create_wo", action="work_order_factory.create", on_error=ErrorPolicy.STOP),
            Step(
                name="notify", 
                action="channels.send_message", 
                template="WO Drafted for {trigger.asset_id} based on {trigger.parameter} alarm. Diagnosis: {diagnose}", 
                on_error=ErrorPolicy.STOP
            ),
        ]
    )
    active_workflows.append(predictive_workflow)

    # Workflow B: Scheduled Preventive Maintenance (Time-Initiated)
    scheduled_pm_workflow = Workflow(
        name="Weekly Extruder Inspection",
        description="Drafts a routine inspection work order every Monday.",
        # Triggered by a cron schedule (e.g., every Monday at 8:00 AM)
        trigger=Trigger(type=TriggerType.SCHEDULE, filter={"cron": "0 8 * * 1"}),
        steps=[
            # Explicitly pass the asset ID for the extruder (e.g., EXT-01)
            Step(
                name="check_history", 
                action="cmms.read_work_orders", 
                inputs={"asset_id": "EXT-01"}, 
                on_error=ErrorPolicy.SKIP
            ),
            Step(
                name="draft_pm", 
                action="work_order_factory.create", 
                # Give the LLM a prompt to synthesize the data into a smart work order
                prompt="Create a PREVENTIVE work order for EXT-01. Note any recurring issues found in the history: {check_history}",
                on_error=ErrorPolicy.STOP
            )
        ]
    )
    active_workflows.append(scheduled_pm_workflow)
    
#  5. The SQL Connector
if ENABLE_SQL:
    # 1. Resolve absolute path to prevent SQLite connection errors
        db_path = SAMPLE_DIR / "custom_cmms.db"
        
        # 1. Create the configuration object FIRST
        sql_config = SqlConnectorConfig(
            dsn=f"Driver={{SQLite3 ODBC Driver}};Database={db_path};", # Note: it expects 'dsn', not 'connection_string'
            driver_type="odbc", # Provide a default, though sqlite bypasses it
            capabilities="read_write", # This replaces 'read_only=False'
            
            # 2. Map the tables. Notice that each table needs an 'entity' name and a 'query'.
            # The connector executes the 'query' and maps the results using 'fields'.
            tables={
                "assets": {
                    "entity": "Asset",
                    "query": "SELECT * FROM plant_equipment",
                    "fields": {
                        "id": {"column": "equip_id"},
                        "name": {"column": "equip_desc"},
                        "type": {"column": "equip_category"},
                        "criticality": {"column": "risk_level"},
                        "equipment_class_code": {"column": "iso_class"},
                        "parent_id": {"column": "parent_equip_id"}
                    }
                },
                "work_orders": {
                    "entity": "WorkOrder",
                    "query": "SELECT * FROM maintenance_tickets",
                    # To enable inserts, we must provide 'insert_table' and 'insert_columns'
                    "insert_table": "maintenance_tickets",
                    "insert_columns": {
                        "id": "ticket_num",
                        "asset_id": "target_equip_id",
                        "type": "job_type",
                        "status": "current_state",
                        "priority": "urgency",
                        "description": "issue_notes",
                        "failure_impact": "functional_impact"
                    },
                    "fields": {
                        "id": {"column": "ticket_num"},
                        "asset_id": {"column": "target_equip_id"},
                        "type": {"column": "job_type"},
                        "status": {"column": "current_state"},
                        "priority": {"column": "urgency"},
                        "description": {"column": "issue_notes"},
                        "failure_impact": {"column": "functional_impact"}
                    }
                },
                "failure_modes": {
                    "entity": "FailureMode",
                    "query": "SELECT * FROM known_faults",
                    "fields": {
                        "code": {"column": "fault_code"},
                        "name": {"column": "fault_label"},
                        "iso_14224_code": {"column": "iso_fault_id"},
                        "mechanism": {"column": "root_cause_mech"},
                        "typical_indicators": {"column": "symptoms_list"},
                        "recommended_actions": {"column": "fix_procedures"}
                    }
                },
            }
        )

        # 3. Pass the config object into the connector
        active_connectors.append(GenericSqlConnector(config=sql_config))
        
        p201_predictive_workflow = Workflow(
            name="P-201 Predictive Bearing Analysis",
            description="Monitor P-201 for climbing temperature/vibration, check its history, and draft a predictive work order before failure.",
            # Triggering on an ALARM for demonstration, but in reality, this could be a TriggerType.SCHEDULE
            trigger=Trigger(
                type=TriggerType.ALARM,
                filter={"asset_id": ["P-201"]}
            ),
            steps=[
                # Step 1: Pull the last 5 sensor readings to see the trend
                Step(
                    name="get_trend",
                    action="sensors.get_related_readings",
                    inputs={"asset_id": "P-201"},
                    on_error=ErrorPolicy.SKIP
                ),
                # Step 2: Read the CMMS to see what broke last time
                Step(
                    name="check_history",
                    action="cmms.read_work_orders",
                    on_error=ErrorPolicy.SKIP
                ),
                # Step 3: Unleash the LLM to connect the dots and write the ticket
                # Step 4: Alert the maintenance team
                Step(
                    name="notify",
                    action="channels.send_message",
                    template="⚠️ PREDICTIVE ALERT: P-201 shows climbing temp/vibration. Work order {draft_wo.id} drafted based on historical bearing failure patterns.",
                    on_error=ErrorPolicy.STOP
                ),
            ]
        )
        active_workflows.append(p201_predictive_workflow)





# Default to llama3 (8B): it reliably handles the tool-calling + synthesis +
# citation contract this agent depends on. Very small models (e.g. 3-4B) often
# can't, and return empty or raw-context answers. Override via --llm with any
# tool-calling model you have pulled (e.g. "ollama:qwen2.5:7b").
def _build_agent(llm: str = "ollama:llama3", sandbox: bool = False) -> Agent:
    """Build the agent with the given LLM and sandbox settings."""
    return Agent(
        name="Maintenance Assistant",
        plant=Plant(name="Demo Plant"),
        connectors=active_connectors,
        channels=[CliChannel()],
        llm=llm,
        workflows=active_workflows,
        sandbox=sandbox,
    )


# ── The entire agent (13 lines) ────────────────────────────────
# The literal definition lives in `_build_agent` above — that is the
# hero of this example. `main()` invokes it with CLI overrides; we
# deliberately avoid building a module-level instance so that
# `python agent.py --help` does not pay the connector construction cost.
# ────────────────────────────────────────────────────────────────


# -- Everything below is optional CLI convenience ----------------


def main() -> None:
    import argparse

    from machina.observability.logging import configure_logging

    parser = argparse.ArgumentParser(description="Machina Quickstart")
    parser.add_argument(
        "--llm",
        default="ollama:llama3",
        help="LLM provider:model (e.g. openai:gpt-4o, anthropic:claude-sonnet-4-20250514)",
    )

    add_mode_flags(parser, default_sandbox=False)
    parser.add_argument("--verbose", action="store_true", help="Debug logging")
    args = parser.parse_args()

    configure_logging(level="DEBUG" if args.verbose else "INFO")
    check(llm=args.llm, sample_dir=SAMPLE_DIR)

    # Quickstart is read-mostly Q&A: default to LIVE so users can experiment freely.
    sandbox = resolve_sandbox(args, default=False)

    # Build agent with CLI overrides
    agent = _build_agent(llm=args.llm, sandbox=sandbox)

    mode = "SANDBOX" if sandbox else "LIVE"
    print(f"\n{'=' * 60}")
    print(f"  Machina Quickstart  |  LLM: {args.llm}  |  Mode: {mode}")
    print(f"{'=' * 60}")
    print()
    print("  Try asking:")
    print('    "What is the bearing replacement procedure for pump P-201?"')
    print('    "Are there spare bearings in stock?"')
    print('    "List all critical assets"')
    print('    "Create a work order for bearing replacement, priority HIGH"')
    print()
    print("  Type 'quit' or Ctrl+C to exit.")
    print(f"{'=' * 60}\n")

    agent.run()


if __name__ == "__main__":
    main()
