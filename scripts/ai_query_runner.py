import json
import duckdb
from ai_sql_guard import get_allowed_assets, validate_sql

DB_PATH = "duckdb/experimentation.duckdb"

def build_llm_context(con) -> str:
    rows = con.execute("""
        select
            asset_name
            , grain
            , primary_keys
            , description
        from dim_ai_allowed_assets
        where is_allowed_for_ai = true
        order by asset_name
    """).fetchall()

    lines = ["Allowed assets (use ONLY these):"]
    for asset_name, grain, primary_keys, description in rows:
        lines.append(f"- {asset_name} | grain: {grain} | keys: {primary_keys} | {description}")
    return "\n".join(lines)

def stub_llm_plan(question: str) -> dict:
    """
    Local stub standing in for an LLM.
    We keep it deterministic and portfolio-friendly.
    """
    q = question.lower().strip()

    if "exposure" in q and "healthy" in q:
        return {
            "question": question,
            "sql": """
                select
                    experiment_id
                    , date_day
                    , exposure_rate
                    , valid_exposure_rate
                    , mismatch_rate
                from _gold.fct_experiment_quality_metrics_daily
                where experiment_id = 'exp_demo_001'
                order by date_day
            """,
            "referenced_assets": ["_gold.fct_experiment_quality_metrics_daily"],
            "notes": "Uses daily quality metrics to assess exposure health."
        }

    if "show me" in q and "raw" in q:
        # intentional unsafe plan (demo)
        return {
            "question": question,
            "sql": "select * from main.sanity_duckdb",
            "referenced_assets": ["main.sanity_duckdb"],
            "notes": "Intentionally unsafe example."
        }

    return {
        "question": question,
        "sql": "",
        "referenced_assets": [],
        "notes": "No plan matched. Add a rule or plug in a real LLM."
    }

def run(question: str):
    con = duckdb.connect(DB_PATH)
    allowed = get_allowed_assets(con)

    context = build_llm_context(con)
    print("\n--- LLM CONTEXT (from semantic contract) ---")
    print(context)

    plan = stub_llm_plan(question)
    print("\n--- QUESTION ---")
    print(plan["question"])
    print("\n--- PLAN (stubbed LLM output) ---")
    print(json.dumps({k: plan[k] for k in ["referenced_assets", "notes"]}, indent=2))
    print("\n--- SQL ---")
    print(plan["sql"].strip())

    if not plan["sql"].strip():
        print("\n❌ No SQL generated.")
        return

    ok, referenced, violations = validate_sql(plan["sql"], allowed)

    print("\n--- VALIDATION ---")
    print("referenced:", referenced)
    if not ok:
        print("❌ BLOCKED. violations:", violations)
        return

    print("✅ Allowed. Executing...\n")
    rows = con.execute(plan["sql"]).fetchall()
    print("Result rows:", rows)

if __name__ == "__main__":
    run("Is exposure tracking healthy for experiment exp_demo_001?")
    run("Show me raw data from sanity table")
