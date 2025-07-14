import json
import os

class RuleParser:
    """
    Parses rule definition JSON files and validates their basic structure.
    """
    def __init__(self):
        pass

    def parse_rule_from_file(self, file_path: str) -> dict:
        """
        Reads and parses a rule definition JSON file from the given path.
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Rule file not found: {file_path}")

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                rule_definition = json.load(f)
            self._validate_basic_rule_structure(rule_definition)
            return rule_definition
        except json.JSONDecodeError as e:
            raise ValueError(f"JSON parsing error in file {file_path}: {e}")
        except ValueError as e:
            raise e 
        except Exception as e:
            raise Exception(f"Unknown error reading rule file {file_path}: {e}")

    def _validate_basic_rule_structure(self, rule: dict):
        """
        Validates the basic structure of the rule definition.
        Ensures required fields exist.
        """
        # Step 1: Check for common fields required in all rules
        if "rule_type" not in rule:
            raise ValueError("Rule definition is missing required field: 'rule_type'.")

        common_required_fields = ["rule_id", "rule_name", "description"]
        for field in common_required_fields:
            if field not in rule:
                raise ValueError(f"Rule definition is missing common required field: '{field}'.")

        # Step 2: Check for type-specific required fields
        rule_type = rule["rule_type"]
        
        type_specific_fields = {
            "VALUE_RANGE": ["pointer", "min_value", "max_value"],
            "VALUE_TEMPLATE": ["pointer", "template_regex"],
            "DATA_CONTINUITY_INTEGRITY": ["pointer", "partition_by_column", "order_by_column"],
            "COMPARISON_SAME_GROUPS_STATISTICAL": ["pointers_1", "pointers_2", "comparison_operator", "join_columns"],
            "COMPARISON_DIFFERENT_GROUPS_STATISTICAL": ["calculation_1", "calculation_2", "comparison_operator", "join_columns"],
            "COMPLEX_BOOLEAN_RULE": ["boolean_expression", "sub_rules_definitions"]
        }

        required_fields = type_specific_fields.get(rule_type)

        if required_fields is None:
            raise ValueError(f"Invalid or unsupported rule type: '{rule_type}'.")

        for field in required_fields:
            if field not in rule:
                raise ValueError(f"Rule definition of type '{rule_type}' is missing required field: '{field}'.")

        # Step 3: (Optional) Deeper validation for specific field formats
        if "pointer" in rule:
            pointer_value = rule["pointer"]
            if len(pointer_value.split('.')) != 3:
                raise ValueError(f"Invalid 'pointer' format (must be 'schema.table.column'): {pointer_value}")
