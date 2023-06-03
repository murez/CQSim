import dataclasses
from typing import Any, Union, get_args, get_origin


def dataclass_types_for_pandas(cls: type):
    """Return a list of dataclass types for a given class."""
    field_types: dict[str, Any] = {}
    for field in dataclasses.fields(cls):
        field_type = field.type
        origin = get_origin(field_type)
        args = get_args(field_type)
        if origin == Union and len(args) == 2 and args[1] == type(None):
            # Optional
            field_type = args[0]
        field_types[field.name] = field_type
    return field_types
