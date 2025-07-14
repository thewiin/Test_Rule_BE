from .base_generator import BaseSqlGenerator

class ComparisonSameGroupsStatisticalGenerator(BaseSqlGenerator):
    """Generates SQL for the COMPARISON_SAME_GROUPS_STATISTICAL rule type."""

    def generate_condition_sql(self, rule_definition: dict) -> str:
        raise NotImplementedError("Comparison rules cannot be used as sub-rules in a complex rule.")

    def generate_sql(self, rule: dict) -> str:
        op = rule['comparison_operator']
        pointers_1 = rule['pointers_1']
        pointers_2 = rule['pointers_2']
        keyword = rule.get('keyword', '').strip()

        table_1 = self._sanitize_sql_identifier(pointers_1['table'])
        table_2 = self._sanitize_sql_identifier(pointers_2['table'])
        
        calc_expr_1 = self._build_calculation_expression(pointers_1['columns'], keyword)
        calc_expr_2 = self._build_calculation_expression(pointers_2['columns'], keyword)

        return f"""
WITH Stat1 AS (
    SELECT {calc_expr_1} as value_1 FROM {table_1}
),
Stat2 AS (
    SELECT {calc_expr_2} as value_2 FROM {table_2}
)
SELECT
    S1.value_1 AS actual_value_1,
    S2.value_2 AS actual_value_2,
    CASE
        WHEN S1.value_1 {op} S2.value_2 THEN 'PASSED'
        ELSE 'FAILED'
    END AS validation_status,
    '{rule['rule_id']}' AS rule_id
FROM
    Stat1 S1, Stat2 S2;
""".strip()
