import pytest

import typing

from crowd.fields import output_fields, output_history_fields
from crowd.togbq import get_table_schema


all_field_lists = [output_fields, output_history_fields]


@pytest.mark.parametrize('field_list', all_field_lists)
def test_field_list_types(field_list):
    # Tests an assumption in get_table_schema that all fields have no more than one arg
    for field_type in field_list.values():
        assert len(typing.get_args(field_type)) <= 1


@pytest.mark.parametrize('field_list', all_field_lists)
def test_bq_schema_generation(field_list):
    # Can the schema be generated without errors?
    # Looking out especially for KeyErrors indicating missing types in the mapping
    schema = get_table_schema(field_list)

    for field in schema:
        print(f"{field.name} ({field.field_type}, mode: {field.mode})")
        print(field.to_api_repr())

