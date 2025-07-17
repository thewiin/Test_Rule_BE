from botocore.exceptions import ValidationError
from flask import Flask, request, jsonify
from jsonschema import validate
import json
from jinja2 import Template
import re, os, psycopg2
from Test_Accuracy_Rule_Engine import RuleEngine

# Khởi tạo Flask và Engine
engine = RuleEngine()
app = Flask(__name__)

# Schema cho validate (tương tự validate_schema.py)
def get_rule_schema(rule_type):
    schemas = {
        "VALUE_RANGE": {
            "type": "object",
            "properties": {
                "rule_id": {"type": "string"},
                "rule_name": {"type": "string"},
                "rule_type": {"type": "string"},
                "description": {"type": "string"},
                "pointer": {"type": "string"},
                "min_value": {"type": "number"},
                "max_value": {"type": "number"},
                "select_columns": {"type": "array", "items": {"type": "string"}}
            },
            "required": ["rule_id", "rule_name", "rule_type", "description", "pointer", "min_value", "max_value"]
        },
        "VALUE_TEMPLATE": {
            "type": "object",
            "properties": {
                "rule_id": {"type": "string"},
                "rule_name": {"type": "string"},
                "rule_type": {"type": "string"},
                "description": {"type": "string"},
                "pointer": {"type": "string"},
                "template_regex": {"type": "string"},
                "calculation": {
                    "type": "object",
                    "properties": {"keyword": {"type": "string"}},
                    "required": ["keyword"]
                },
                "select_columns": {"type": "array", "items": {"type": "string"}}
            },
            "required": ["rule_id", "rule_name", "rule_type", "description", "pointer", "template_regex", "calculation"]
        },
        "DATA_CONTINUITY_INTEGRITY": {
            "type": "object",
            "properties": {
                "rule_id": {"type": "string"},
                "rule_name": {"type": "string"},
                "rule_type": {"type": "string"},
                "description": {"type": "string"},
                "pointer": {"type": "string"},
                "partition_by_column": {"type": "string"},
                "order_by_column": {"type": "string"},
                "select_columns": {"type": "array", "items": {"type": "string"}}
            },
            "required": ["rule_id", "rule_name", "rule_type", "description", "pointer", "partition_by_column", "order_by_column"]
        },
        "COMPARISON_SAME_GROUPS_STATISTICAL": {
            "type": "object",
            "properties": {
                "rule_id": {"type": "string"},
                "rule_name": {"type": "string"},
                "rule_type": {"type": "string"},
                "description": {"type": "string"},
                "comparison_operator": {"type": "string"},
                "join_columns": {"type": "array", "items": {"type": "string"}},
                "pointers_1": {
                    "type": "object",
                    "properties": {"table": {"type": "string"}, "columns": {"type": "array", "items": {"type": "string"}}},
                    "required": ["table", "columns"]
                },
                "pointers_2": {
                    "type": "object",
                    "properties": {"table": {"type": "string"}, "columns": {"type": "array", "items": {"type": "string"}}},
                    "required": ["table", "columns"]
                },
                "select_columns": {"type": "array", "items": {"type": "string"}}
            },
            "required": ["rule_id", "rule_name", "rule_type", "description", "comparison_operator", "join_columns", "pointers_1", "pointers_2"]
        },
        "COMPARISON_DIFFERENT_GROUPS_STATISTICAL": {
            "type": "object",
            "properties": {
                "rule_id": {"type": "string"},
                "rule_name": {"type": "string"},
                "rule_type": {"type": "string"},
                "description": {"type": "string"},
                "comparison_operator": {"type": "string"},
                "join_columns": {"type": "array", "items": {"type": "string"}},
                "calculation_1": {
                    "type": "object",
                    "properties": {"table": {"type": "string"}, "columns": {"type": "array", "items": {"type": "string"}}, "keyword": {"type": "string"}},
                    "required": ["table", "columns"]
                },
                "calculation_2": {
                    "type": "object",
                    "properties": {"table": {"type": "string"}, "columns": {"type": "array", "items": {"type": "string"}}, "keyword": {"type": "string"}},
                    "required": ["table", "columns"]
                },
                "select_columns": {"type": "array", "items": {"type": "string"}}
            },
            "required": ["rule_id", "rule_name", "rule_type", "description", "comparison_operator", "join_columns", "calculation_1", "calculation_2"]
        },
        "COMPLEX_BOOLEAN_RULE": {
            "type": "object",
            "properties": {
                "rule_id": {"type": "string"},
                "rule_name": {"type": "string"},
                "rule_type": {"type": "string"},
                "description": {"type": "string"},
                "boolean_expression": {"type": "string"},
                "sub_rules_definitions": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "rule_id": {"type": "string"},
                            "rule_name": {"type": "string"},
                            "rule_type": {"type": "string"},
                            "description": {"type": "string"},
                            "pointer": {"type": "string"},
                            "min_value": {"type": "number"},
                            "max_value": {"type": "number"},
                            "template_regex": {"type": "string"},
                            "calculation": {
                                "type": "object",
                                "properties": {"keyword": {"type": "string"}},
                                "required": ["keyword"]
                            }
                        },
                        "required": ["rule_id", "rule_name", "rule_type", "description"]
                    }
                },
                "select_columns": {"type": "array", "items": {"type": "string"}}
            },
            "required": ["rule_id", "rule_name", "rule_type", "description", "boolean_expression", "sub_rules_definitions"]
        }
    }
    return schemas.get(rule_type, {})

mock_rules = {}
mock_results = {}

# Create rule file
def create_sample_rule_file(rule_name: str, content: dict):
    rules_dir = "rules"
    os.makedirs(rules_dir, exist_ok=True)
    rule_path = os.path.join(rules_dir, f"{rule_name}.json")
    with open(rule_path, "w", encoding="utf-8") as f:
        json.dump(content, f, indent=2, ensure_ascii=False)
    return rule_path

# Upload Rule
@app.route('/upload-rule', methods=['POST'])
def upload_rule():
    try:
        rule = request.get_json()
        rule_type = rule.get("rule_type")
        validate(instance=rule, schema=get_rule_schema(rule_type))
        rule_id = rule.get("rule_id", rule.get("complex_rule_id"))
        mock_rules[rule_id] = rule
        create_sample_rule_file(rule_id, rule)
        return jsonify({"statusCode": 200, "body": {"rule": rule}})
    except Exception as e:
        return jsonify({"statusCode": 500, "error": str(e)}), 500

# Validate Response
@app.route('/validate-response', methods=['GET'])
def validate_response():
    rule_id = request.args.get('rule_id')
    if not rule_id:
        return jsonify({"status": "Error", "message": "Missing rule_id parameter"}), 400

    if rule_id in mock_rules:
        rule = mock_rules[rule_id]
        validation_message = f"Validation for {rule_id} successful"
        if rule.get("rule_type") == "COMPLEX_BOOLEAN_RULE":
            validation_message += " (complex rule validated)"
        return jsonify({"status": "OK", "message": validation_message})
    return jsonify({"status": "Error", "message": f"Rule with ID {rule_id} not found"}), 400

# Predict ETA
@app.route('/predict-eta', methods=['POST'])
def predict_eta():
    try:
        data = request.get_json()
        if not data or "rule_type" not in data:
            return jsonify({"status": "error", "message": "Missing rule_type"}), 400

        rule_type = data["rule_type"]
        row_count = data.get("row_count", 1000000)  # Default row count
        if row_count <= 0:
            return jsonify({"status": "error", "message": "row_count must be positive"}), 400

        base_times = {
            "VALUE_RANGE": 1.0,
            "VALUE_TEMPLATE": 1.5,
            "DATA_CONTINUITY_INTEGRITY": 3.0,
            "COMPARISON_SAME_GROUPS_STATISTICAL": 5.0,
            "COMPARISON_DIFFERENT_GROUPS_STATISTICAL": 7.0,
            "COMPLEX_BOOLEAN_RULE": 10.0
        }
        base_time = base_times.get(rule_type)
        if base_time is None:
            return jsonify({"status": "error", "message": f"Unsupported rule_type: {rule_type}"}), 400

        eta = base_time * (row_count / 1000000)
        return jsonify({"eta": round(eta, 2)})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# Generate SQL
@app.route('/generate-sql', methods=['POST'])
def generate_sql():
    try:
        rule = request.get_json()
        if not rule or "rule_type" not in rule:
            return jsonify({"status": "error", "message": "Missing rule_type"}), 400

        rule_id = rule.get("rule_id", rule.get("complex_rule_id", "temp_rule"))
        rule_path = create_sample_rule_file(rule_id, rule)
        parsed_rule = engine.parser.parse_rule_from_file(rule_path)
        sql = engine.generator.generate_sql(parsed_rule)
        return jsonify(sql)
    except ValueError as ve:
        return jsonify({"status": "error", "message": str(ve)}), 400
    except NotImplementedError as nie:
        return jsonify({"status": "error", "message": str(nie)}), 501
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# Execute SQL
@app.route('/execute-sql', methods=['POST'])
def execute_sql():
    try:
        data = request.get_json()
        if not data or "sql" not in data:
            return jsonify({"status": "error", "message": "Missing sql field"}), 400

        sql = data["sql"].strip()
        if not sql:
            return jsonify({"status": "error", "message": "SQL query is empty"}), 400

        rule = data.get("rule", {})
        violations = 1 if "WHERE" in sql else 0
        return jsonify([{"violation": True} for _ in range(violations)])
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# Determine Result
@app.route('/determine-result', methods=['POST'])
def determine_result():
    try:
        data = request.get_json()
        if "violation_count" not in data or "threshold" not in data:
            return jsonify({"status": "error", "message": "Missing violation_count or threshold"}), 400

        violation_count = data["violation_count"]
        threshold = data["threshold"]
        if not isinstance(violation_count, (int, float)) or not isinstance(threshold, (int, float)):
            return jsonify({"status": "error", "message": "violation_count and threshold must be numbers"}), 400

        result = "PASS" if violation_count <= threshold else "FAIL"
        return jsonify({"result": result})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# Log Result
@app.route('/log-result', methods=['POST'])
def log_result():
    try:
        data = request.get_json()
        if "key" not in data or "result" not in data:
            return jsonify({"status": "error", "message": "Missing key or result"}), 400

        key = data["key"]
        result = data["result"]
        if not isinstance(key, str) or not result:
            return jsonify({"status": "error", "message": "Invalid key or result format"}), 400

        mock_results[key] = result
        return jsonify({"status": "success"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# Get Result
@app.route('/get-result', methods=['GET'])
def get_result():
    try:
        rule_id = request.args.get('rule_id')
        if not rule_id:
            return jsonify({"status": "error", "message": "Missing rule_id parameter"}), 400

        if rule_id not in mock_rules:
            return jsonify({"status": "error", "message": f"Rule with ID {rule_id} not found"}), 400

        result_key = rule_id
        result = mock_results.get(result_key, "FAIL")
        violations = 2 if result == "FAIL" else 0

        return jsonify({
            "status": result,
            "time": 5,
            "violations": violations,
            "download_link": f"http://localhost:5000/results/{rule_id}"
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# Load Function to Database
@app.route('/load-function', methods=['POST'])
def load_function():
    try:
        data = request.get_json()
        if not data or "function_name" not in data or "function_body" not in data:
            return jsonify({"status": "error", "message": "Missing function_name or function_body"}), 400

        function_name = data["function_name"]
        function_body = data["function_body"]
        engine.load_function_to_db(function_name, function_body)
        return jsonify({"status": "success", "message": f"Function {function_name} loaded"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)