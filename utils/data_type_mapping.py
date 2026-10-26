MODBUS_TO_PG_TYPE = {
    # Integers
    "int8": "DOUBLE PRECISION",
    "uint8": "DOUBLE PRECISION",
    "int16": "DOUBLE PRECISION",
    "uint16": "DOUBLE PRECISION",
    "int32": "DOUBLE PRECISION",
    "uint32": "DOUBLE PRECISION",

    # Floating
    "float32": "DOUBLE PRECISION",
    "float64": "DOUBLE PRECISION",

    # Boolean
    "bool": "BOOLEAN",

    # Text / fallback
    "string": "TEXT",
    "text": "TEXT",

    # Timestamp
    "timestamp": "TIMESTAMP UNIQUE"
}
