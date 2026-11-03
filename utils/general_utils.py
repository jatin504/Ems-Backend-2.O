"""Small reusable helpers shared across the application.

Keep general, stateless utilities here; domain services belong in their own modules.
"""


def parse_tags(value):
    """None means all tags; an explicit empty selection means no tags."""
    if value is None:
        return None
    if isinstance(value, str):
        value = value.split(",")
    if not isinstance(value, list) or any(not isinstance(tag, str) for tag in value):
        raise ValueError("tags must be a comma-separated string or a list of strings")
    return {tag.strip().casefold() for tag in value if tag.strip()}


def select_tags(data, tags):
    """Keep original keys and values, matching requested names ignoring case."""
    if tags is None:
        return data
    if not isinstance(data, dict):
        return {}
    return {key: value for key, value in data.items() if key.casefold() in tags}
