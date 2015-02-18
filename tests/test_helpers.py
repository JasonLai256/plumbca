# -*- coding:utf-8 -*-
"""
    tests.helpers
    ~~~~~~~~~~~~~

    :copyright: (c) 2015 by Jason Lai.
    :license: BSD, see LICENSE for more details.
"""

import pytest

from plumbca.helpers import find_ge, find_lt, find_eq


def test_find_func(metadata_t):
    '''testing the helper functions that using bisect mosule.'''
    # [1] ->
    item = [143373]
    assert find_ge(metadata_t, item) == [168011, 'hello', 123]
    assert find_ge(metadata_t, item, True) == 2
    assert find_lt(metadata_t, item) == [143372, 'hello', 1234]
    assert find_lt(metadata_t, item, True) == 1
    with pytest.raises(ValueError):
        find_eq(metadata_t, item)

    # [2] ->
    item = [143372]
    assert find_ge(metadata_t, item) == [143372, 'hello', 123]
    assert find_ge(metadata_t, item, True) == 0
    with pytest.raises(ValueError):
        find_lt(metadata_t, item)
    with pytest.raises(ValueError):
        find_eq(metadata_t, item)

    # [3] ->
    item = [228072]
    assert find_ge(metadata_t, item) == [228072, 'hello', 123]
    assert find_ge(metadata_t, item, True) == 5
    assert find_lt(metadata_t, item) == [188072, 'hello', 123]
    assert find_lt(metadata_t, item, True) == 4
    with pytest.raises(ValueError):
        find_eq(metadata_t, item)

    # [4] ->
    item = [228072, 'hello', 123]
    assert find_ge(metadata_t, item) == [228072, 'hello', 123]
    assert find_ge(metadata_t, item, True) == 5
    assert find_lt(metadata_t, item) == [188072, 'hello', 123]
    assert find_lt(metadata_t, item, True) == 4
    assert find_eq(metadata_t, item) == [228072, 'hello', 123]
    assert find_eq(metadata_t, item, True) == 5

    # [5] ->
    item = [558072]
    with pytest.raises(ValueError):
        rv = find_ge(metadata_t, item)
    assert find_lt(metadata_t, item) == [228072, 'hello', 123]
    assert find_lt(metadata_t, item, True) == 5
    with pytest.raises(ValueError):
        rv = find_eq(metadata_t, item)
