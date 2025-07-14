from .base_generator import BaseSqlGenerator

class ComparisonDifferentGroupsStatisticalGenerator(BaseSqlGenerator):
    """Generates SQL for the COMPARISON_DIFFERENT_GROUPS_STATISTICAL rule type."""
    
    def generate_condition_sql(self, rule_definition: dict) -> str:
        raise NotImplementedError("Comparison rules cannot be used as sub-rules in a complex rule.")

    def generate_sql(self, rule: dict) -> str:
        op = rule['comparison_operator']
        join_columns = rule['join_columns']
        calc_1_def = rule['calculation_1']
        calc_2_def = rule['calculation_2']

        table_1 = self._sanitize_sql_identifier(calc_1_def['table'])
        table_2 = self._sanitize_sql_identifier(calc_2_def['table'])

        calc_expr_1 = self._build_calculation_expression(calc_1_def['columns'], calc_1_def.get('keyword', ''))
        calc_expr_2 = self._build_calculation_expression(calc_2_def['columns'], calc_2_def.get('keyword', ''))
        
        join_conditions = ' AND '.join(
            f"T1.{self._sanitize_sql_identifier(col)} = T2.{self._sanitize_sql_identifier(col)}" for col in join_columns
        )
        
        select_clause = self._build_select_clause(rule.get('select_columns'), 'T1')

        return f"""
SELECT
    {select_clause}
    {', '.join(f'T1.{self._sanitize_sql_identifier(col)}' for col in join_columns)} AS join_key,
    T1.{calc_expr_1} AS calculated_value_1,
    T2.{calc_expr_2} AS calculated_value_2,
    CASE
        WHEN T1.{calc_expr_1} {op} T2.{calc_expr_2} THEN 'PASSED'
        ELSE 'FAILED'
    END AS validation_status,
    '{rule['rule_id']}' AS rule_id
FROM
    {table_1} T1
JOIN
    {table_2} T2 ON {join_conditions};
""".strip()
