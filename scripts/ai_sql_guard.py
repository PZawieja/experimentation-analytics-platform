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
    Pragmatic parser:
    - captures table/view identifiers after FROM/JOIN
    - supports both: schema.table and bare_table
    - ignores subquery parentheses and commas in FROM lists
    """
    # captures: from <ident> or join <ident>, where ident can be a.b or a
    pattern = r"(?:from|join)\s+([a-zA-Z0-9_]+(?:\.[a-zA-Z0-9_]+)?)"
    assets = set()

    for m in re.finditer(pattern, sql, flags=re.IGNORECASE):
        ident = m.group(1).strip().lower()
        # strip trailing punctuation if any
        ident = re.sub(r"[;,)]$", "", ident)
        assets.add(ident)

    return assets

def validate_sql(sql: str, allowed_assets: set[str]) -> tuple[bool, list[str], list[str]]:
    s = sql.strip().lower()

    violations = []

    # 1) allow SELECT only
    if not s.startswith("select") and not s.startswith("with"):
        violations.append("only_select_allowed")

    # 2) block wildcard
    if re.search(r"select\s+\*", s):
        violations.append("select_star_blocked")

    # 3) block dangerous keywords (small, high-signal list)
    blocked = [
        "attach", "detach", "copy", "export", "import", "pragma",
        "install", "load", "call", "create", "drop", "alter", "update",
        "delete", "insert", "merge"
    ]
    for kw in blocked:
        if re.search(rf"\b{kw}\b", s):
            violations.append(f"blocked_keyword:{kw}")

    # 4) allowlisted assets only
    referenced = sorted(extract_referenced_assets(sql))
    not_allowed = [a for a in referenced if a not in allowed_assets]
    if not_allowed:
        violations.extend([f"non_allowlisted_asset:{a}" for a in not_allowed])

    ok = len(violations) == 0
    return (ok, referenced, violations)

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
