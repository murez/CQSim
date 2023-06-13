import dataclasses
from typing import Any, Union, get_args, get_origin, get_type_hints


def dataclass_types(cls: type):
    """
    Return a list of dataclass types for a given class.
    """
    resolved_hints = get_type_hints(cls)
    field_types: dict[str, Any] = {}
    for field in dataclasses.fields(cls):
        field_type = field.type

        # https://stackoverflow.com/questions/51946571/how-can-i-get-python-3-7-new-dataclass-field-types
        if isinstance(field_type, str):
            if resolved := resolved_hints.get(field.name):
                field_type = resolved

        field_types[field.name] = field_type

    return field_types


def dataclass_types_for_pandas(cls: type):
    """
    Return a list of dataclass types for a given class.
    For pandas means get origin and try to get first arg if it's a union.
    """
    resolved_hints = get_type_hints(cls)
    field_types: dict[str, Any] = {}
    for field in dataclasses.fields(cls):
        field_type = field.type

        # https://stackoverflow.com/questions/51946571/how-can-i-get-python-3-7-new-dataclass-field-types
        if isinstance(field_type, str):
            if resolved := resolved_hints.get(field.name):
                field_type = resolved

        origin = get_origin(field_type)
        args = get_args(field_type)
        if origin == Union and len(args) == 2 and args[1] == type(None):
            # Optional
            field_type = args[0]
        field_types[field.name] = field_type
    return field_types
