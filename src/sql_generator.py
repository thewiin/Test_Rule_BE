from src.sql_generators.value_range_generator import ValueRangeSqlGenerator
from src.sql_generators.value_template_generator import ValueTemplateSqlGenerator
from src.sql_generators.data_continuity_integrity_generator import DataContinuityIntegritySqlGenerator
from src.sql_generators.comparison_same_groups_statistical_generator import ComparisonSameGroupsStatisticalGenerator
from src.sql_generators.comparison_different_groups_statistical_generator import ComparisonDifferentGroupsStatisticalGenerator
from src.sql_generators.complex_boolean_rule_generator import ComplexBooleanRuleSqlGenerator

class SqlGenerator:
    """
    Dispatches rule definitions to appropriate specific SQL generators.
    """
    def __init__(self):
        self.generators = {
            "VALUE_RANGE": ValueRangeSqlGenerator(),
            "VALUE_TEMPLATE": ValueTemplateSqlGenerator(),
            "DATA_CONTINUITY_INTEGRITY": DataContinuityIntegritySqlGenerator(),
            "COMPARISON_SAME_GROUPS_STATISTICAL": ComparisonSameGroupsStatisticalGenerator(),
            "COMPARISON_DIFFERENT_GROUPS_STATISTICAL": ComparisonDifferentGroupsStatisticalGenerator(),
            "COMPLEX_BOOLEAN_RULE": ComplexBooleanRuleSqlGenerator(),
        }

    def generate_sql(self, rule_definition: dict) -> str:
        """
        Generates SQL script based on the rule definition's rule_type.

        Args:
            rule_definition (dict): Dictionary representing the parsed rule definition.

        Returns:
            str: The generated SQL script.

        Raises:
            ValueError: If rule_type is missing or unsupported.
            NotImplementedError: If the specific generator for the rule_type is not implemented.
        """
        rule_type = rule_definition.get("rule_type")
        if not rule_type:
            raise ValueError("Rule definition is missing 'rule_type'.")

        generator = self.generators.get(rule_type)
        if not generator:
            raise NotImplementedError(f"SQL generation for rule type '{rule_type}' is not implemented.")

        return generator.generate_sql(rule_definition)

