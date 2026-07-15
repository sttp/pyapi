#!/usr/bin/env python3
# ******************************************************************************************************
#  test_metadata_compression.py - Pytest tests for publisher metadata/SIC compression negotiation
#
#  Copyright © 2026, Grid Protection Alliance.  All Rights Reserved.
#
#  Licensed to the Grid Protection Alliance (GPA) under one or more contributor license agreements.
# ******************************************************************************************************
#
#  Regression tests for the publisher compression handshake. The publisher must GZip-compress
#  metadata (and the signal index cache) only when the subscriber negotiates BOTH the content flag
#  (COMPRESSMETADATA / COMPRESSSIGNALINDEXCACHE) AND the GZIP compression mode -- matching the STTP
#  C# reference (DataPublisher.SerializeMetadata / SerializeSignalIndexCache).
#
#  Previously the publisher compressed whenever the content flag was set, ignoring the GZip bit.
#  A .NET DataSubscriber requests COMPRESSMETADATA without advertising GZIP by default, so it then
#  received GZip bytes it would not inflate and failed to parse the metadata (0x1F "invalid
#  character"). See src/sttp/transport/subscriberconnection.py (should_gzip_compress).
#
# ******************************************************************************************************

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

import pytest

from sttp.transport.subscriberconnection import should_gzip_compress
from sttp.transport.constants import OperationalModes, CompressionModes

META = int(OperationalModes.COMPRESSMETADATA)
SIC = int(OperationalModes.COMPRESSSIGNALINDEXCACHE)
GZIP = int(CompressionModes.GZIP)
NOISE = 0x00000002 | 0x00010000  # version + an arbitrary unrelated bit


@pytest.mark.parametrize("content_flag", [
    OperationalModes.COMPRESSMETADATA,
    OperationalModes.COMPRESSSIGNALINDEXCACHE,
])
def test_compress_requires_flag_and_gzip(content_flag):
    flag = int(content_flag)

    # Both the content flag and GZip negotiated -> compress.
    assert should_gzip_compress(flag | GZIP, content_flag) is True

    # Content flag set but GZip NOT negotiated -> do NOT compress (the .NET-default case).
    assert should_gzip_compress(flag, content_flag) is False

    # GZip negotiated but the content flag not set -> do NOT compress.
    assert should_gzip_compress(GZIP, content_flag) is False

    # Neither negotiated -> do NOT compress.
    assert should_gzip_compress(0, content_flag) is False


def test_flags_are_independent():
    # GZip + only the metadata flag compresses metadata but not the signal index cache.
    modes = META | GZIP
    assert should_gzip_compress(modes, OperationalModes.COMPRESSMETADATA) is True
    assert should_gzip_compress(modes, OperationalModes.COMPRESSSIGNALINDEXCACHE) is False

    # GZip + only the SIC flag compresses the signal index cache but not metadata.
    modes = SIC | GZIP
    assert should_gzip_compress(modes, OperationalModes.COMPRESSSIGNALINDEXCACHE) is True
    assert should_gzip_compress(modes, OperationalModes.COMPRESSMETADATA) is False


def test_unrelated_bits_do_not_affect_decision():
    # Version/encoding and other unrelated bits must not trigger or suppress compression.
    assert should_gzip_compress(META | GZIP | NOISE, OperationalModes.COMPRESSMETADATA) is True
    assert should_gzip_compress(META | NOISE, OperationalModes.COMPRESSMETADATA) is False


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
