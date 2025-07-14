from .base_generator import BaseSqlGenerator

class ComplexBooleanRuleSqlGenerator(BaseSqlGenerator):
    """
    Handles the COMPLEX_BOOLEAN_RULE type by generating a single, unified SQL query.
    """
    def generate_condition_sql(self, rule_definition: dict) -> str:
        raise NotImplementedError("A complex rule cannot be a sub-rule of another complex rule.")

    def generate_sql(self, rule: dict) -> str:
        from src.sql_generator import SqlGenerator
        sql_generator = SqlGenerator()

        boolean_expression = rule['boolean_expression']
        sub_rules = rule['sub_rules_definitions']

        if not sub_rules:
            raise ValueError("Complex rule must have at least one sub-rule.")

        # --- Assumption: All sub-rules operate on the same primary table. ---
        # We determine the primary table from the first sub-rule.
        first_sub_rule = sub_rules[0]
        if 'pointer' not in first_sub_rule:
            raise ValueError("Sub-rules within a complex rule must have a 'pointer' to determine the primary table.")
        
        schema, table, _ = self._parse_pointer(first_sub_rule['pointer'])
        primary_table = f"{self._sanitize_sql_identifier(schema)}.{self._sanitize_sql_identifier(table)}"

        # --- Generate condition SQL for each sub-rule ---
        conditions = {}
        for sub_rule in sub_rules:
            sub_rule_id = sub_rule['rule_id']
            rule_type = sub_rule['rule_type']
            
            # Get the specific generator for the sub-rule type
            generator = sql_generator.generators.get(rule_type)
            if not generator:
                raise NotImplementedError(f"SQL generation for sub-rule type '{rule_type}' is not implemented.")
            
            # Generate the boolean condition string for the sub-rule
            condition_sql = generator.generate_condition_sql(sub_rule)
            conditions[sub_rule_id] = condition_sql
        
        # --- Replace placeholders in the boolean expression with actual SQL conditions ---
        final_condition = f" {boolean_expression} "
        for rule_id, condition in conditions.items():
            # Use word boundaries to ensure we replace whole words only
            final_condition = final_condition.replace(f" {rule_id} ", f" ({condition}) ")

        final_condition = final_condition.strip()
        
        # --- Build the final, single SQL query ---
        select_clause = self._build_select_clause(rule.get('select_columns'))

        return f"""
SELECT
    {select_clause}
    CASE
        WHEN {final_condition} THEN 'PASSED'
        ELSE 'FAILED'
    END AS validation_status,
    '{rule['rule_id']}' AS rule_id
FROM
    {primary_table};
""".strip()
