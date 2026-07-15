# Cross-language metadata date/time interop harness

Verifies that the STTP **Python publisher**'s XML metadata `xs:dateTime` serialization is accepted
by the STTP **C# subscriber** (the `gsfapi` reference implementation). It spins up a real Python
publisher in-process and runs the real C# subscriber against it, exchanging XML metadata that
includes date/time values.

## Why this exists

The Python publisher previously serialized timezone-aware UTC datetimes as a lexically invalid
string carrying both a `+00` offset and a `Z` designator (e.g. `2033-03-03T03:33:33.123+00:Z`).
Python's lenient parser accepts it; the C# subscriber's `.NET DataSet.ReadXml` rejects it. The fix
in [`src/sttp/data/dataset.py`](../../src/sttp/data/dataset.py) (`xsdformat` / `_value_to_xml_text`)
emits a **bare** `xs:dateTime` (no `Z`, no offset), matching the C# reference publisher.

The harness seeds the served metadata with a fixed **timezone-aware UTC** `UpdatedOn` sentinel
(`2033-03-03T03:33:33.123Z`) — the exact input that used to fail — and asserts it round-trips to
the C# subscriber.

## Prerequisites

- **Python**: the repo's virtual environment (`numpy`, `python-dateutil`). The script imports the
  worktree's `src`, so the fix under test is exercised.
- **C# build**: Roslyn `csc.exe` (Visual Studio 2022 or Build Tools) and the prebuilt STTP library
  assemblies under `<gsfapi>\build\output\{Release,Debug}\lib\` (must contain `sttp.gsf.dll`). The
  script compiles the C# sample [`InteropTest-gsf/Program.cs`](file:///C:/Projects/sttp/gsfapi/src/samples/InteropTest-gsf/Program.cs)
  against that self-consistent assembly set.

  > Build note: a full `msbuild sttp.gsf.sln` from source also works in a normal Visual Studio
  > environment. This harness compiles only `Program.cs` with `csc` because the bundled
  > `depends\GSF` DLLs can be out of sync with the library source (a `[Label]` attribute-target
  > mismatch breaks the from-source library build in some environments).

## Running

```
# From the pyapi worktree root, using the project venv:
python test/interop/run_metadata_interop.py            # inject tz-aware UTC sentinel (the fix)
python test/interop/run_metadata_interop.py --baseline # stock (naive) metadata only
python test/interop/run_metadata_interop.py --keep      # keep _artifacts/metadata-received.xml
python test/interop/run_metadata_interop.py --port 7180 # override the publisher port
```

Exit code `0` = interop succeeded (C# received and parsed the metadata; sentinel datetime found).
Non-zero = failure (build error, C# deserialization error, timeout, or value mismatch).

### Environment overrides

- `STTP_GSFAPI_ROOT` — path to the `gsfapi` repo (default `C:\Projects\sttp\gsfapi`).
- `STTP_CSC` — path to `csc.exe` (default: discovered via `vswhere`).

## How it detects the bug

The C# `--metadata` mode (added to `Program.cs`) requests metadata on connect, then:

- on **`MetaDataReceived`** — writes `metadata-received.xml`, prints each `DateTime` column value,
  and exits `0`;
- on **`ProcessException`** — the malformed datetime makes `DataSet.ReadXml` throw, surfaced as
  "Failed to process publisher response packet …"; the process exits non-zero;
- on **timeout** (15 s) — exits non-zero.

To confirm the harness catches the regression, temporarily restore the old serializer in
`_value_to_xml_text` and re-run: the C# side reports
`The string '2033-03-03T03:33:33.123+00:Z' is not a valid AllXsd value` and the harness fails.

## Note: metadata compression handshake

The C# `--metadata` mode explicitly requests `CompressionModes.GZip`. The Python publisher
GZip-compresses metadata whenever the `CompressMetadata` operational mode is set (on by default),
but the C# subscriber only inflates metadata when the GZip compression bit is *also* advertised.
Without this, the C# side receives raw gzip bytes and fails with a `0x1F` "invalid character"
error. This is a **separate** operational-mode handshake nuance, independent of the date fix.
