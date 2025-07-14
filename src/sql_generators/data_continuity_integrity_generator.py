from .base_generator import BaseSqlGenerator

class DataContinuityIntegritySqlGenerator(BaseSqlGenerator):
    """Generates SQL for the DATA_CONTINUITY_INTEGRITY rule type."""

    def generate_condition_sql(self, rule_definition: dict) -> str:
        raise NotImplementedError("DATA_CONTINUITY_INTEGRITY rules cannot be used as sub-rules in a complex rule.")

    def generate_sql(self, rule: dict) -> str:
        schema, table, id_column = self._parse_pointer(rule["pointer"])
        full_table_name = f"{self._sanitize_sql_identifier(schema)}.{self._sanitize_sql_identifier(table)}"
        sanitized_id_column = self._sanitize_sql_identifier(id_column)
        
        partition_by = self._sanitize_sql_identifier(rule["partition_by_column"])
        order_by = self._sanitize_sql_identifier(rule["order_by_column"])
        step = rule.get("sequence_step_size", 1)

        select_clause = self._build_select_clause(rule.get('select_columns'), 't')

        return f"""
WITH LaggedData AS (
    SELECT
        {select_clause}
        {sanitized_id_column} AS current_id,
        LAG({sanitized_id_column}, 1) OVER (PARTITION BY {partition_by} ORDER BY {order_by}) AS previous_id
    FROM
        {full_table_name} t
)
SELECT
    {self._build_select_clause(rule.get('select_columns'))}
    current_id,
    previous_id,
    CASE
        WHEN current_id = COALESCE(previous_id, current_id - {self._format_sql_value(step)}) + {self._format_sql_value(step)} THEN 'PASSED'
        ELSE 'FAILED'
    END AS validation_status,
    '{rule['rule_id']}' AS rule_id
FROM
    LaggedData;
""".strip()

