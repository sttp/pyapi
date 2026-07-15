# Cross-language metadata interop harness

Verifies interop between the STTP **Python publisher** and the STTP **C# subscriber** (the `gsfapi`
reference implementation) for XML metadata exchange. It spins up a real Python publisher in-process
and runs the real C# subscriber against it. It covers two fixes:

1. **`xs:dateTime` serialization** — the Python publisher previously serialized timezone-aware UTC
   datetimes as a lexically invalid string carrying both a `+00` offset and a `Z` designator (e.g.
   `2033-03-03T03:33:33.123+00:Z`). Python's lenient parser accepts it; the C# subscriber's `.NET
   DataSet.ReadXml` rejects it. The fix in [`src/sttp/data/dataset.py`](../../src/sttp/data/dataset.py)
   (`xsdformat` / `_value_to_xml_text`) emits a **bare** `xs:dateTime`, matching the C# reference.

2. **Metadata compression handshake** — the Python publisher previously GZip-compressed metadata
   whenever `CompressMetadata` was set, ignoring the negotiated GZip mode. The `.NET DataSubscriber`
   default requests `CompressMetadata` **without** GZip and so receives gzip bytes it will not
   inflate, failing on the `0x1F` magic byte. The fix in
   [`src/sttp/transport/subscriberconnection.py`](../../src/sttp/transport/subscriberconnection.py)
   (`should_gzip_compress`) compresses only when both the content flag **and** GZip are negotiated,
   matching the C# reference (`DataPublisher.SerializeMetadata` / `SerializeSignalIndexCache`).

The harness seeds the served metadata with a fixed **timezone-aware UTC** `UpdatedOn` sentinel
(`2033-03-03T03:33:33.123Z`) — the exact input that used to fail — and, by default, runs the
exchange in **both** compression negotiations (uncompressed and GZip), asserting the sentinel
round-trips in each.

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
python test/interop/run_metadata_interop.py                      # sentinel, BOTH compression modes
python test/interop/run_metadata_interop.py --compression plain  # only the uncompressed path
python test/interop/run_metadata_interop.py --compression gzip   # only the compressed path
python test/interop/run_metadata_interop.py --baseline           # stock (naive) metadata
python test/interop/run_metadata_interop.py --keep               # keep _artifacts/metadata-received.xml
python test/interop/run_metadata_interop.py --port 7180          # override the base publisher port
```

By default both compression negotiations run (each on its own port); both must succeed. Exit code
`0` = every selected exchange succeeded (C# parsed the metadata; sentinel datetime found). Non-zero
= failure (build error, C# deserialization error, timeout, or value mismatch).

### Environment overrides

- `STTP_GSFAPI_ROOT` — path to the `gsfapi` repo (default `C:\Projects\sttp\gsfapi`).
- `STTP_CSC` — path to `csc.exe` (default: discovered via `vswhere`).

## How it detects a regression

The C# `InteropTest HOSTNAME PORT --metadata [--gzip]` mode (added to `Program.cs`) requests
metadata on connect, then:

- on **`MetaDataReceived`** — writes `metadata-received.xml`, prints each `DateTime` column value,
  and exits `0`;
- on **`ProcessException`** — a malformed datetime or undecodable (unexpectedly gzip-compressed)
  payload makes `DataSet.ReadXml` throw, surfaced as "Failed to process publisher response packet …";
  the process exits non-zero;
- on **timeout** (15 s) — exits non-zero.

Without `--gzip` the subscriber uses the `.NET`-default operational modes (`CompressMetadata`
without GZip); with `--gzip` it advertises GZip. The orchestrator drives both.

Both fixes have teeth — revert one and re-run:

- **date fix** (`_value_to_xml_text`) → C# reports
  `The string '2033-03-03T03:33:33.123+00:Z' is not a valid AllXsd value`;
- **compression fix** (`should_gzip_compress`) → with `--compression plain` the publisher compresses
  anyway and C# fails with `hexadecimal value 0x1F, is an invalid character`.
