#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for the location module.
"""
import pytest

from location import location


def test_something():
    assert True


def test_with_error():
    with pytest.raises(ValueError):
        # Do something that raises a ValueError
        raise(ValueError)


# Fixture example
@pytest.fixture
def an_object():
    return {}


def test_location(an_object):
    assert an_object == {}
