"""在实际最低解释器解析公开注解，不能只靠延迟字符串伪装兼容。"""

import importlib
import inspect
import typing

import pytest


@pytest.mark.parametrize(
    "name",
    ["tansr_sdk", "tansr_sdk.api", "tansr_sdk.session", "tansr_sdk.executor", "tansr_sdk.archive", "tansr_sdk.storage", "tansr_sdk.memory_publication"],
)
def test_public_annotations_resolve(name):
    module = importlib.import_module(name)
    exports = module.__all__
    assert len(exports) == len(set(exports))
    for symbol in exports:
        value = getattr(module, symbol)
        if inspect.isfunction(value) or inspect.isclass(value):
            typing.get_type_hints(value)
        if inspect.isclass(value):
            for method_name, method in inspect.getmembers(value):
                if not method_name.startswith("_") and inspect.isfunction(method):
                    typing.get_type_hints(method)
