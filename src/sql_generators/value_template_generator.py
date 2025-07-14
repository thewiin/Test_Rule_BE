from .base_generator import BaseSqlGenerator

class ValueTemplateSqlGenerator(BaseSqlGenerator):
    """Generates SQL for the VALUE_TEMPLATE rule type."""

    def generate_condition_sql(self, rule: dict) -> str:
        """Generates the boolean condition for a regex template check."""
        _, _, column = self._parse_pointer(rule["pointer"])
        sanitized_column = self._sanitize_sql_identifier(column)
        regex_keyword = self._sanitize_sql_identifier(rule.get("calculation", {}).get("keyword"))
        template_regex = self._format_sql_value(rule['template_regex'])
        
        if not regex_keyword:
            raise ValueError("VALUE_TEMPLATE rule is missing 'calculation.keyword'.")
            
        return f"{regex_keyword}({sanitized_column}, {template_regex})"

    def generate_sql(self, rule: dict) -> str:
        """Generates a full query to evaluate all rows for the regex template."""
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
