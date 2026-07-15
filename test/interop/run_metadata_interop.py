#!/usr/bin/env python3
# ******************************************************************************************************
#  run_metadata_interop.py - Cross-language metadata date/time interop harness
#
#  Copyright © 2026, Grid Protection Alliance.  All Rights Reserved.
#
#  Licensed to the Grid Protection Alliance (GPA) under one or more contributor license agreements.
# ******************************************************************************************************
#
#  Spins up a real STTP Python publisher and a real STTP C# subscriber (the gsfapi "InteropTest"
#  sample, run in --metadata mode) and exercises XML metadata exchange that includes date/time
#  values. It verifies that the Python publisher's xs:dateTime serialization is accepted by the
#  .NET DataSet parser used by the C# subscriber.
#
#  The publisher's served metadata is seeded with a *timezone-aware UTC* UpdatedOn value. That is
#  the exact input that, before the fix in src/sttp/data/dataset.py, produced the malformed
#  "...+00:Z" lexical string that C# rejects. With the fix, the value is emitted as a bare
#  xs:dateTime and the C# subscriber deserializes it successfully.
#
#  Exit code: 0 = interop succeeded (C# received and parsed the metadata, sentinel datetime found),
#             non-zero = failure (build error, C# deserialization error, timeout, or value mismatch).
#
#  Usage:
#      python test/interop/run_metadata_interop.py [--baseline] [--keep] [--port N]
#
#      --baseline   Serve stock (naive) metadata without injecting a tz-aware value. Use this to
#                   confirm the C# subscriber can receive Python metadata at all, isolating the
#                   date issue from any operational-mode/gzip handshake problem.
#      --keep       Do not delete the C# output directory (metadata-received.xml) on exit.
#      --port N     TCP port for the publisher (default 7169).
#
#  Environment overrides:
#      STTP_GSFAPI_ROOT   Path to the gsfapi repo (default: C:\\Projects\\sttp\\gsfapi)
#      STTP_CSC           Path to csc.exe (default: discovered via vswhere)
#
#  Build note: this harness compiles the C# Program.cs with csc against the prebuilt assemblies in
#  <gsfapi>\build\output\{Release,Debug}\lib. That folder is a self-consistent assembly set. A full
#  "msbuild sttp.gsf.sln" from source also works in a normal Visual Studio environment; it is not
#  used here because the bundled depends\GSF DLLs can be out of sync with the library source.
#
# ******************************************************************************************************

import argparse
import os
import subprocess
import sys
import time
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
PYAPI_ROOT = os.path.abspath(os.path.join(HERE, '..', '..'))
SRC_DIR = os.path.join(PYAPI_ROOT, 'src')

# Ensure the worktree's src (with the fix) is imported, not any installed sttpapi package.
sys.path.insert(0, SRC_DIR)

GSFAPI_ROOT = os.environ.get('STTP_GSFAPI_ROOT', r'C:\Projects\sttp\gsfapi')
PROGRAM_CS = os.path.join(GSFAPI_ROOT, 'src', 'samples', 'InteropTest-gsf', 'Program.cs')
METADATA_XML = os.path.join(PYAPI_ROOT, 'examples', 'simplepublish', 'Metadata.xml')

# A distinctive, fixed timezone-aware UTC value so we can assert its round-trip in the C# output.
# Sub-second precision is included to exercise fractional-second handling.
SENTINEL = datetime(2033, 3, 3, 3, 33, 33, 123000, tzinfo=timezone.utc)
SENTINEL_SECONDS = '2033-03-03T03:33:33'  # what the C# dump prints (to-seconds prefix)


def _log(msg):
    print(f'[HARNESS] {msg}', flush=True)


def _find_csc():
    """Locate the Roslyn csc.exe (needed for the C# 9+ syntax in Program.cs)."""
    override = os.environ.get('STTP_CSC')
    if override and os.path.isfile(override):
        return override

    vswhere = r'C:\Program Files (x86)\Microsoft Visual Studio\Installer\vswhere.exe'
    if os.path.isfile(vswhere):
        try:
            out = subprocess.run(
                [vswhere, '-latest', '-products', '*', '-requires', 'Microsoft.Component.MSBuild',
                 '-find', r'MSBuild\**\Bin\Roslyn\csc.exe'],
                capture_output=True, text=True, check=False).stdout.strip()
            first = out.splitlines()[0].strip() if out else ''
            if first and os.path.isfile(first):
                return first
        except Exception:
            pass

    raise FileNotFoundError(
        'Could not locate Roslyn csc.exe. Set STTP_CSC to its path, or build the C# sample with '
        'Visual Studio / msbuild first.')


def _prebuilt_lib_dir():
    for config in ('Release', 'Debug'):
        lib = os.path.join(GSFAPI_ROOT, 'build', 'output', config, 'lib')
        if os.path.isfile(os.path.join(lib, 'sttp.gsf.dll')):
            return lib
    raise FileNotFoundError(
        f'Could not find prebuilt sttp.gsf.dll under {GSFAPI_ROOT}\\build\\output\\*\\lib. '
        'Build the gsfapi library once (Visual Studio / msbuild) to produce it.')


def build_csharp_exe():
    """Compile Program.cs into <lib>/InteropTest.exe against the prebuilt assembly set."""
    lib = _prebuilt_lib_dir()
    csc = _find_csc()
    exe = os.path.join(lib, 'InteropTest.exe')

    if not os.path.isfile(PROGRAM_CS):
        raise FileNotFoundError(f'C# sample source not found: {PROGRAM_CS}')

    _log(f'Compiling C# subscriber with csc -> {exe}')
    cmd = [
        csc, '-nologo', '-target:exe', '-langversion:latest', f'-out:{exe}',
        '-r:' + os.path.join(lib, 'sttp.gsf.dll'),
        '-r:' + os.path.join(lib, 'GSF.Core.dll'),
        '-r:' + os.path.join(lib, 'GSF.TimeSeries.dll'),
        '-r:' + os.path.join(lib, 'GSF.Communication.dll'),
        PROGRAM_CS,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if result.returncode != 0 or not os.path.isfile(exe):
        _log('csc output:\n' + (result.stdout or '') + (result.stderr or ''))
        raise RuntimeError('C# compilation failed.')
    return exe, lib


def start_publisher(port, inject_sentinel):
    """Start an in-process Python publisher serving metadata (optionally seeded with the sentinel)."""
    from sttp.publisher import Publisher
    from sttp.data.dataset import DataSet

    with open(METADATA_XML, 'r') as f:
        metadata_xml = f.read()

    metadata, err = DataSet.from_xml(metadata_xml)
    if err:
        raise RuntimeError(f'Failed to load metadata: {err}')

    if inject_sentinel:
        table = metadata.table('DeviceDetail')
        if table is None or table.rowcount == 0:
            raise RuntimeError('DeviceDetail table with rows is required to inject the sentinel.')
        set_err = table.row(0).set_value_byname('UpdatedOn', SENTINEL)
        if set_err is not None:
            raise RuntimeError(f'Failed to set sentinel UpdatedOn: {set_err}')
        _log(f'Injected timezone-aware UTC UpdatedOn = {SENTINEL.isoformat()} into DeviceDetail[0]')

    publisher = Publisher()
    publisher.statusmessage_logger = lambda m: print(f'[PUB] {m}', flush=True)
    publisher.errormessage_logger = lambda m: print(f'[PUB ERROR] {m}', file=sys.stderr, flush=True)
    publisher.clientconnected_receiver = lambda c: _log(f'C# client connected: {c.connection_id}')
    publisher.start(port)
    publisher.define_metadata(metadata)
    _log(f'Publisher listening on port {port}')
    return publisher


def run(args):
    exe, lib = build_csharp_exe()

    # C# writes metadata-received.xml into its working directory.
    out_dir = os.path.join(HERE, '_artifacts')
    os.makedirs(out_dir, exist_ok=True)

    publisher = None
    try:
        publisher = start_publisher(args.port, inject_sentinel=not args.baseline)
        time.sleep(0.5)  # let the listener settle

        _log(f'Launching C# subscriber: InteropTest.exe localhost {args.port} --metadata')
        proc = subprocess.run(
            [exe, 'localhost', str(args.port), '--metadata'],
            cwd=out_dir, capture_output=True, text=True, timeout=45)

        stdout = proc.stdout or ''
        stderr = proc.stderr or ''
        print('----- C# stdout -----')
        print(stdout, end='')
        if stderr.strip():
            print('----- C# stderr -----')
            print(stderr, end='')
        print('---------------------')
        _log(f'C# subscriber exit code: {proc.returncode}')

        ok = proc.returncode == 0
        if not ok:
            _log('FAIL: C# subscriber exited non-zero (metadata deserialization error or timeout).')
            return 1

        if not args.baseline:
            if SENTINEL_SECONDS in stdout:
                _log(f'PASS: sentinel datetime {SENTINEL_SECONDS} round-tripped to the C# subscriber.')
            else:
                _log(f'FAIL: sentinel datetime {SENTINEL_SECONDS} not found in C# output.')
                return 1
        else:
            _log('PASS (baseline): C# subscriber received and parsed Python metadata.')

        return 0
    finally:
        if publisher is not None:
            try:
                publisher.stop()
            except Exception as ex:
                _log(f'Error stopping publisher: {ex}')
        if not args.keep:
            received = os.path.join(out_dir, 'metadata-received.xml')
            try:
                if os.path.isfile(received):
                    os.remove(received)
                if os.path.isdir(out_dir) and not os.listdir(out_dir):
                    os.rmdir(out_dir)
            except OSError as ex:
                _log(f'Cleanup warning (ignored): {ex}')
        else:
            _log(f'Artifacts kept in {out_dir}')


def main():
    parser = argparse.ArgumentParser(description='STTP Python-publisher / C#-subscriber metadata interop harness')
    parser.add_argument('--baseline', action='store_true',
                        help='Serve stock (naive) metadata without the tz-aware sentinel')
    parser.add_argument('--keep', action='store_true', help='Keep the C# output directory')
    parser.add_argument('--port', type=int, default=7169, help='Publisher TCP port (default 7169)')
    args = parser.parse_args()

    try:
        rc = run(args)
    except Exception as ex:
        _log(f'ERROR: {ex}')
        rc = 2

    _log('RESULT: ' + ('SUCCESS' if rc == 0 else f'FAILURE (exit {rc})'))
    sys.exit(rc)


if __name__ == '__main__':
    main()
