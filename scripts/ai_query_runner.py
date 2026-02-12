"""
Small demo runner for AI-style analytics Q&A in DuckDB.

- Prints the allowlisted assets ("semantic contract") for the AI.
- Uses stubbed LLM logic: either parameterized safe templates or a direct SQL plan.
- Validates SQL against the allowlist before execution.
- Executes and prints results, with a short interpretation for the template path.
"""

import json
import duckdb
from ai_intents import SQL_TEMPLATES
from ai_sql_guard import get_allowed_assets, validate_sql
from tabulate import tabulate

DB_PATH = "duckdb/experimentation.duckdb"

SQL_DID_TREATMENT_WIN = """
select
    experiment_id
  , n_control
  , cr_control
  , n_treatment
  , cr_treatment
  , uplift_abs
  , z_score
  , p_value
  , ci_low
  , ci_high
  , case
        when p_value < {alpha}
         and uplift_abs > 0
            then true
        else false
    end as did_treatment_win
from ai_fct_experiment_results
where experiment_id = '{experiment_id}'
"""


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


def stub_llm_params(question: str) -> dict:
    """
    Stub that returns parameters only (no SQL).
    This is the safe pattern for analytics Q&A.
    """
    q = question.lower().strip()

    if "did" in q and "treatment" in q and "win" in q:
        return {
            "question": question,
            "intent": "did_treatment_win",
            "asset_name": "ai_fct_experiment_results",
            "params": {
                "experiment_id": "exp_demo_001",
                "alpha": 0.05
            },
            "notes": "Parameters for 'did treatment win?' using allowlisted results view."
        }

    return {
        "question": question,
        "intent": "",
        "asset_name": "",
        "params": {},
        "notes": "No params matched."
    }


def run(question: str):
    con = duckdb.connect(DB_PATH)
    allowed = get_allowed_assets(con)

    context = build_llm_context(con)
    print("\n--- LLM CONTEXT (from semantic contract) ---")
    print(context)

    params_plan = stub_llm_params(question)
    if params_plan["asset_name"] == "ai_fct_experiment_results":
        intent = params_plan["intent"]
        if intent not in SQL_TEMPLATES:
            print("❌ Unknown intent:", intent)
            return
        sql = SQL_TEMPLATES[intent].format(**params_plan["params"])

        print("\n--- QUESTION ---")
        print(params_plan["question"])
        print("\n--- PLAN (stubbed LLM output: params only) ---")
        print(json.dumps({k: params_plan[k] for k in ["asset_name", "params", "notes"]}, indent=2))
        print("\n--- SQL (fixed template) ---")
        print(sql.strip())

        ok, referenced, violations = validate_sql(sql, allowed)
        expected_asset = params_plan["asset_name"].lower()
        if referenced != [expected_asset]:
            print("❌ BLOCKED. SQL must reference only the declared asset.")
            print("expected:", expected_asset)
            print("got:", referenced)
            return

        print("\n--- VALIDATION ---")
        print("referenced:", referenced)
        if not ok:
            print("❌ BLOCKED. violations:", violations)
            return

        print("✅ Allowed. Executing...\n")
        rows = con.execute(sql).fetchall()
        cols = [d[0] for d in con.description]

        print(tabulate(rows, headers=cols, tablefmt="github"))

        if rows:
            row = dict(zip(cols, rows[0]))

            print("\n--- INTERPRETATION ---")

            alpha = params_plan["params"]["alpha"]

            if row["did_treatment_win"]:
                print(
                    f"✅ Treatment WON.\n"
                    f"- Uplift: {row['uplift_abs']:.4f}\n"
                    f"- p-value: {row['p_value']:.6f} < alpha ({alpha})\n"
                    f"- 95% CI: [{row['ci_low']:.4f}, {row['ci_high']:.4f}] (does not include 0)"
                )
            else:
                print(
                    f"❌ Treatment did NOT win.\n"
                    f"- Uplift: {row['uplift_abs']:.4f}\n"
                    f"- p-value: {row['p_value']:.6f} >= alpha ({alpha})\n"
                    f"- 95% CI: [{row['ci_low']:.4f}, {row['ci_high']:.4f}] "
                    f"{'(includes 0)' if row['ci_low'] <= 0 <= row['ci_high'] else ''}"
                )

                if row["n_control"] < 30 or row["n_treatment"] < 30:
                    print("⚠️ Very small sample size — results are unstable.")

        return


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
    run("Did treatment win for experiment exp_demo_001?")
