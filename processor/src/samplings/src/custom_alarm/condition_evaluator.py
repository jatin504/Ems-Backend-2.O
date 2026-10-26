class ConditionEvaluator:

    @staticmethod
    def evaluate(value, data_type, condition):
        ctype = condition["type"]

        if data_type == "number":
            return ConditionEvaluator._number(value, condition)

        if data_type == "boolean":
            return ConditionEvaluator._boolean(value, condition)

        if data_type in ("string", "enum"):
            return ConditionEvaluator._string(value, condition)

        raise ValueError(f"Unsupported data type: {data_type}")
    
    @staticmethod
    def _number(value, condition):
        v = condition.get("value")

        match condition["type"]:
            case "greater_than":
                return value > v
            case "greater_than_equal":
                return value >= v
            case "less_than":
                return value < v
            case "less_than_equal":
                return value <= v
            case "equal":
                return value == v
            case "not_equal":
                return value != v
            case "between":
                low, high = condition["range"]
                return low <= value <= high
            case _:
                raise ValueError(f"Invalid number condition: {condition['type']}")
            
    @staticmethod
    def _number(value, condition):
        v = condition.get("value")

        match condition["type"]:
            case "greater_than":
                return value > v
            case "greater_than_equal":
                return value >= v
            case "less_than":
                return value < v
            case "less_than_equal":
                return value <= v
            case "equal":
                return value == v
            case "not_equal":
                return value != v
            case "between":
                low, high = condition["range"]
                return low <= value <= high
            case _:
                raise ValueError(f"Invalid number condition: {condition['type']}")

    @staticmethod
    def _boolean(value, condition):
        expected = condition.get("value")

        match condition["type"]:
            case "equal":
                return value is expected
            case "not_equal":
                return value is not expected
            case _:
                raise ValueError(f"Invalid boolean condition: {condition['type']}")

    @staticmethod
    def _string(value, condition):
        ctype = condition["type"]

        match ctype:
            case "equal":
                return value == condition["value"]
            case "not_equal":
                return value != condition["value"]
            case "in":
                return value in condition["values"]
            case "not_in":
                return value not in condition["values"]
            case "regex":
                import re
                return re.match(condition["pattern"], value) is not None
            case _:
                raise ValueError(f"Invalid string condition: {ctype}")
