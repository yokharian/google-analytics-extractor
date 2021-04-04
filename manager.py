from .connector_factory import ConnectorFactory
from .decorators import custom_base_mapping


# noinspection PyMissingOrEmptyDocstring
@custom_base_mapping(var_name="_mapping")
class FactoryManager:
    @classmethod
    def get_factory(cls, alias: str) -> ConnectorFactory:
        """obtain appropriate factory based on self.alias mapping"""
        concrete_factory = cls._mapping.get(alias)
        if not concrete_factory:
            raise NotImplementedError(f"handler not found for {cls}")
        return concrete_factory
