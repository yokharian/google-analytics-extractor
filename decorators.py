from functools import wraps
from typing import Union

_mapping = {}


def mapping_alias(connector: Union[str, list]):
    """class decorator to save the relation (alias-class)
    inside the class to which it belongs.
    variable name is assigned with the custom_base_mapping decorator"""

    @wraps(connector)
    def decorator(class_: object):
        """custom accumulator functionality"""
        if type(connector) is list:
            for c in connector:
                _mapping[c] = class_
        elif type(connector) is str:
            _mapping[connector] = class_
        else:
            raise SyntaxError(f"{mapping_alias} must be used with list or str")
        return class_

    return decorator


def custom_base_mapping(var_name: str):
    """function decorator to save the relation (alias-function)
    inside the class to which it belongs.
    relation is assigned with the mapping_alias class decorator"""

    @wraps(var_name)
    def decorator(cls: object):
        """custom accumulator functionality"""
        if not var_name:
            raise SyntaxError(f"{custom_base_mapping} has invalid var_name")

        setattr(cls, var_name, _mapping)
        return cls

    return decorator
