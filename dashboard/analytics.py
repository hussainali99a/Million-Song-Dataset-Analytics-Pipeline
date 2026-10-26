from __future__ import annotations


def quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def qualified_name(schema: str, table: str) -> str:
    return f"{quote_identifier(schema)}.{quote_identifier(table)}"


def find_column(columns: list[str], *names: str) -> str | None:
    by_lower = {name.lower(): name for name in columns}
    return next((by_lower[name.lower()] for name in names if name.lower() in by_lower), None)


def deduplicated_artist_join(
    fact_alias: str,
    source_key: str,
    dimension: tuple[tuple[str, str], str, str],
) -> str:
    (schema, table), dimension_key, dimension_name = dimension
    normalized_dimension_key = (
        f"NULLIF(TRIM(CAST({quote_identifier(dimension_key)} AS VARCHAR)), '')"
    )
    return (
        f" LEFT JOIN ("
        f"SELECT {normalized_dimension_key} AS artist_join_key, "
        f"MAX(NULLIF(TRIM(CAST({quote_identifier(dimension_name)} AS VARCHAR)), '')) "
        f"AS artist_join_name "
        f"FROM {qualified_name(schema, table)} "
        f"WHERE {normalized_dimension_key} IS NOT NULL "
        f"GROUP BY {normalized_dimension_key}"
        f") AS artist_dim ON "
        f"NULLIF(TRIM(CAST({fact_alias}.{quote_identifier(source_key)} AS VARCHAR)), '') "
        "= artist_dim.artist_join_key"
    )
