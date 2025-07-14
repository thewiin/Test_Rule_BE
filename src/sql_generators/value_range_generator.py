from .base_generator import BaseSqlGenerator

class ValueRangeSqlGenerator(BaseSqlGenerator):
    """Generates SQL for the VALUE_RANGE rule type."""

    def generate_condition_sql(self, rule: dict) -> str:
        """Generates the boolean condition for a value range check."""
        _, _, column = self._parse_pointer(rule["pointer"])
        sanitized_column = self._sanitize_sql_identifier(column)
        min_val = self._format_sql_value(rule['min_value'])
        max_val = self._format_sql_value(rule['max_value'])
        return f"({sanitized_column} >= {min_val} AND {sanitized_column} <= {max_val})"

    def generate_sql(self, rule: dict) -> str:
        """Generates a full query to evaluate all rows for the value range."""
        schema, table, _ = self._parse_pointer(rule["pointer"])
        full_table_name = f"{self._sanitize_sql_identifier(schema)}.{self._sanitize_sql_identifier(table)}"
        
        condition = self.generate_condition_sql(rule)
        select_clause = self._build_select_clause(rule.get('select_columns'))

        return f"""
SELECT
    {select_clause}
    CASE
        WHEN {condition} THEN 'PASSED'
        ELSE 'FAILED'
    END AS validation_status,
    '{rule['rule_id']}' AS rule_id
FROM
    {full_table_name};
""".strip()
