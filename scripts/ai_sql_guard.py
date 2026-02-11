import re
import duckdb

DB_PATH = "duckdb/experimentation.duckdb"

def get_allowed_assets(con) -> set[str]:
    rows = con.execute("""
        select asset_name
        from dim_ai_allowed_assets
        where is_allowed_for_ai = true
    """).fetchall()
    return {r[0].lower() for r in rows}

def extract_referenced_assets(sql: str) -> set[str]:
    """
    Very small, pragmatic parser:
    - captures schema.table occurrences after FROM/JOIN
    - we keep it simple for now (portfolio demo)
    """
    pattern = r"(?:from|join)\s+([a-zA-Z0-9_]+\.[a-zA-Z0-9_]+)"
    return {m.group(1).lower() for m in re.finditer(pattern, sql, flags=re.IGNORECASE)}

def validate_sql(sql: str, allowed_assets: set[str]) -> tuple[bool, list[str], list[str]]:
    referenced = sorted(extract_referenced_assets(sql))
    violations = [a for a in referenced if a not in allowed_assets]
    return (len(violations) == 0, referenced, violations)

def run_query(sql: str):
    con = duckdb.connect(DB_PATH)
    allowed = get_allowed_assets(con)
    ok, referenced, violations = validate_sql(sql, allowed)

    print("\nSQL:")
    print(sql)
    print("\nReferenced assets:", referenced)

    if not ok:
        print("\n❌ BLOCKED: SQL references non-allowed assets:")
        for v in violations:
            print(" -", v)
        raise SystemExit(2)

    print("\n✅ Allowed. Executing...\n")
    rows = con.execute(sql).fetchall()
    print("Result rows:", rows)

if __name__ == "__main__":
    # Golden Question #1 (allowed):
    # "Is exposure tracking healthy for experiment exp_demo_001?"
    allowed_sql = """
    select
        experiment_id
        , date_day
        , exposure_rate
        , valid_exposure_rate
        , mismatch_rate
    from _gold.fct_experiment_quality_metrics_daily
    where experiment_id = 'exp_demo_001'
    order by date_day
    """

    run_query(allowed_sql)

    # Golden Question #2 (blocked on purpose):
    # tries to query a non-allowlisted table
    blocked_sql = """
    select *
    from main.sanity_duckdb
    """

    run_query(blocked_sql)
