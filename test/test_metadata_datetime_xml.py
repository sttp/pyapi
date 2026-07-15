#!/usr/bin/env python3
# ******************************************************************************************************
#  test_metadata_datetime_xml.py - Pytest tests for xs:dateTime metadata serialization
#
#  Copyright © 2026, Grid Protection Alliance.  All Rights Reserved.
#
#  Licensed to the Grid Protection Alliance (GPA) under one or more contributor license agreements.
# ******************************************************************************************************
#
#  Regression tests for the metadata date/time serialization fix. The Python publisher must emit a
#  bare xs:dateTime lexical value (no "Z" designator and no UTC offset) so the STTP C# subscriber,
#  which parses metadata via .NET DataSet.ReadXml, accepts it and round-trips it without a timezone
#  shift. The prior implementation produced strings such as "...+00:Z" for timezone-aware UTC values,
#  which C# rejects. See src/sttp/data/dataset.py (xsdformat / _value_to_xml_text).
#
# ******************************************************************************************************

import os
import sys
from datetime import datetime, timezone, timedelta

# Add src to path (parent directory)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

import pytest

from sttp.data.dataset import DataSet, xsdformat
from sttp.data.datatype import DataType
from sttp.ticks import Ticks

METADATA_PATH = os.path.join(os.path.dirname(__file__),
                             '..', 'examples', 'simplepublish', 'Metadata.xml')


def _assert_bare(text: str):
    """A canonical xs:dateTime carries no timezone designator or offset."""
    assert 'Z' not in text, f"unexpected 'Z' designator in {text!r}"
    assert '+' not in text, f"unexpected offset in {text!r}"
    assert '+00' not in text, f"unexpected '+00' offset in {text!r}"
    # Never leave a dangling decimal point from fractional-second trimming
    assert not text.endswith('.'), f"dangling decimal point in {text!r}"


@pytest.mark.parametrize("value, expected", [
    # timezone-aware UTC, whole second -> bare, no fractional part
    (datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc), '2024-01-01T00:00:00'),
    # timezone-aware UTC with milliseconds
    (datetime(2024, 1, 1, 12, 34, 56, 789000, tzinfo=timezone.utc), '2024-01-01T12:34:56.789'),
    # timezone-aware non-UTC (-06:00) -> normalized to UTC wall-clock
    (datetime(2026, 2, 7, 21, 27, 24, 282000, tzinfo=timezone(timedelta(hours=-6))),
     '2026-02-08T03:27:24.282'),
    # naive with trailing zeros -> trimmed, no dangling dot
    (datetime(2024, 1, 1, 12, 34, 56, 100000), '2024-01-01T12:34:56.1'),
    # naive whole second -> no fractional part, no dangling dot
    (datetime(2024, 1, 1, 12, 34, 56), '2024-01-01T12:34:56'),
    # naive with full microsecond precision -> preserved
    (datetime(2024, 1, 1, 12, 34, 56, 123456), '2024-01-01T12:34:56.123456'),
])
def test_xsdformat_bare_output(value, expected):
    """xsdformat emits a bare xs:dateTime for both tz-aware and naive inputs."""
    text = xsdformat(value)
    assert text == expected
    _assert_bare(text)


def test_value_to_xml_text_datetime_is_bare():
    """The metadata serializer path emits the same bare form for tz-aware UTC values."""
    value = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    text = DataSet._value_to_xml_text(value, DataType.DATETIME)
    assert text == '2024-01-01T00:00:00'
    _assert_bare(text)


def test_ticks_utcnow_serializes_bare():
    """A programmatic (tz-aware UTC) datetime from Ticks must serialize bare.

    Ticks.to_datetime()/Ticks.utcnow() produce timezone-aware UTC datetimes; this is the
    exact input that previously produced the malformed '...+00:Z' string.
    """
    value = Ticks.to_datetime(Ticks.utcnow())
    assert value.tzinfo is not None, "precondition: Ticks datetime is timezone-aware"
    _assert_bare(xsdformat(value))
    _assert_bare(DataSet._value_to_xml_text(value, DataType.DATETIME))


def test_to_xml_injected_tzaware_utc_roundtrips():
    """End-to-end: inject a tz-aware UTC UpdatedOn, serialize, and confirm the XML is bare
    and round-trips through DataSet.from_xml without a shift."""
    with open(METADATA_PATH, 'r') as f:
        metadata_xml = f.read()

    metadata, err = DataSet.from_xml(metadata_xml)
    assert not err, f"failed to load metadata: {err}"

    # Inject a timezone-aware UTC value (the previously-failing path) with sub-millisecond
    # precision to also exercise fidelity of the fractional component.
    injected = datetime(2024, 1, 1, 12, 34, 56, 123456, tzinfo=timezone.utc)
    table = metadata.table("DeviceDetail")
    assert table is not None and table.rowcount > 0, "DeviceDetail rows required for test"
    row = table.row(0)
    set_err = row.set_value_byname("UpdatedOn", injected)
    assert set_err is None, f"failed to set UpdatedOn: {set_err}"

    xml = metadata.to_xml()

    # The serialized field must be bare, and the document must contain neither the malformed
    # '+00:Z' string nor any offset/designator on the injected value.
    assert '2024-01-01T12:34:56.123456' in xml, "injected datetime not serialized as bare form"
    assert '+00:Z' not in xml
    assert '+00' not in xml
    assert '123456Z' not in xml and 'T12:34:56.123456+' not in xml

    # Round-trip: reload and confirm the value survives (as naive UTC wall-clock).
    reloaded, err = DataSet.from_xml(xml)
    assert not err, f"failed to reload metadata: {err}"
    value, verr = reloaded.table("DeviceDetail").row(0).value_byname("UpdatedOn")
    assert verr is None, f"failed to read UpdatedOn: {verr}"
    assert value == injected.astimezone(timezone.utc).replace(tzinfo=None)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
