# STTP Publisher Python Port - Architecture Mapping

This document describes the mapping between the C++ publisher implementation and the Python port.

## Overview

The Python publisher implementation is a feature-complete port of the C++ STTP publisher, maintaining wire protocol compatibility while following Python idioms and conventions.

## C++ to Python Class/Module Mapping

### Core Publisher Components

| C++ Class | C++ Location | Python Module | Notes |
|-----------|--------------|---------------|-------|
| `DataPublisher` | `cppapi/src/lib/transport/DataPublisher.{h,cpp}` | `pyapi/src/sttp/transport/datapublisher.py` | Main publisher class managing connections and metadata |
| `SubscriberConnection` | `cppapi/src/lib/transport/SubscriberConnection.{h,cpp}` | `pyapi/src/sttp/transport/subscriberconnection.py` | Individual client connection handler |
| `PublisherInstance` | `cppapi/src/lib/transport/PublisherInstance.{h,cpp}` | `pyapi/src/sttp/publisher.py` | User-facing wrapper, similar to `Subscriber` pattern |
| `RoutingTables` | `cppapi/src/lib/transport/RoutingTables.{h,cpp}` | `pyapi/src/sttp/transport/routingtables.py` | Routes measurements to subscribed connections |

### Supporting Components (Shared with Subscriber)

| C++ Class | C++ Location | Python Module | Notes |
|-----------|--------------|---------------|-------|
| `SignalIndexCache` | `cppapi/src/lib/transport/SignalIndexCache.{h,cpp}` | `pyapi/src/sttp/transport/signalindexcache.py` | Already exists (shared) |
| `CompactMeasurement` | `cppapi/src/lib/transport/CompactMeasurement.{h,cpp}` | `pyapi/src/sttp/transport/compactmeasurement.py` | Already exists (shared) |
| `Measurement` | `cppapi/src/lib/transport/TransportTypes.h` | `pyapi/src/sttp/transport/measurement.py` | Already exists (shared) |
| `Constants` | `cppapi/src/lib/transport/Constants.{h,cpp}` | `pyapi/src/sttp/transport/constants.py` | Already exists (shared) |
| `TSSC Encoder` | `cppapi/src/lib/transport/tssc/TSSCEncoder.{h,cpp}` | `pyapi/src/sttp/transport/tssc/encoder.py` | New - counterpart to decoder |

### Metadata Components (Shared)

| C++ Class | C++ Location | Python Module | Notes |
|-----------|--------------|---------------|-------|
| `DataSet` | `cppapi/src/lib/data/DataSet.{h,cpp}` | `pyapi/src/sttp/data/dataset.py` | Already exists (shared) |
| `DataTable` | `cppapi/src/lib/data/DataTable.{h,cpp}` | `pyapi/src/sttp/data/datatable.py` | Already exists (shared) |
| `FilterExpression` | `cppapi/src/lib/filterexpressions/` | `pyapi/src/sttp/data/filterexpressionparser.py` | Already exists (shared) |

## Architectural Flow

### Connection Lifecycle (Server Mode)

1. **Initialization**: `DataPublisher.__init__()` sets up state
2. **Start**: `DataPublisher.start(port)` creates TCP listener socket
3. **Accept Loop**: Background thread accepts connections via `_accept_connection()`
4. **Handshake**: Each connection creates a `SubscriberConnection` instance
5. **Protocol Negotiation**: Via command channel messages (already defined in constants)
6. **Subscription**: Client sends subscribe request, publisher builds routing table
7. **Publication**: `publish_measurements()` routes data via `RoutingTables`
8. **Shutdown**: `stop()` closes all connections gracefully

### Key Differences from C++ Implementation

1. **Threading Model**:
   - C++: Uses Boost.Asio async I/O with thread pools
   - Python: Uses `asyncio` for I/O, `ThreadPoolExecutor` for callbacks (similar to existing subscriber)

2. **Memory Management**:
   - C++: Shared pointers (`SharedPtr<T>`)
   - Python: Native garbage collection, weak references where needed

3. **Networking**:
   - C++: Boost.Asio sockets
   - Python: Standard `socket` library (consistent with subscriber)

4. **Timers**:
   - C++: Boost.Asio timers
   - Python: `threading.Timer` (consistent with subscriber)

## Protocol Parity

The Python implementation maintains **full wire protocol parity** with C++:

- Same command/response byte sequences (defined in `constants.py`)
- Same compact measurement encoding
- Same TSSC compression algorithm
- Same metadata XML schema
- Same operational modes and encoding flags
- Same cipher key rotation for security

## Implementation Strategy

### Phase 1: Core Publisher Infrastructure
- `DataPublisher` class with connection management
- `SubscriberConnection` class for individual clients
- Command channel protocol handlers

### Phase 2: Data Publication
- `RoutingTables` for measurement routing
- TSSC encoder implementation
- Compact measurement serialization (publisher side)

### Phase 3: User-Facing API
- `Publisher` wrapper class (mirrors `Subscriber`)
- Metadata definition helpers
- Example applications

### Phase 4: Testing & Verification
- Python publisher → Python subscriber
- Python publisher → C++ subscriber
- Wire protocol validation

## Intentional Deviations

All deviations from C++ implementation follow the same patterns as the existing Python subscriber:

1. **Snake_case naming**: All Python code uses snake_case (e.g., `publish_measurements` vs `PublishMeasurements`)
2. **Properties vs Getters/Setters**: Python uses `@property` decorators
3. **Callbacks**: Python uses simple callable assignment vs registration methods
4. **Exceptions**: Python uses standard exception hierarchy vs custom C++ exceptions
5. **Async patterns**: Python uses asyncio/threading consistently with subscriber

## File Organization

```
pyapi/src/sttp/
├── publisher.py                          # NEW: User-facing Publisher class
├── transport/
│   ├── datapublisher.py                  # NEW: Core publisher
│   ├── subscriberconnection.py           # NEW: Connection handler
│   ├── routingtables.py                  # NEW: Measurement routing
│   ├── tssc/
│   │   └── encoder.py                    # NEW: TSSC encoder
│   ├── datasubscriber.py                 # EXISTING: Subscriber
│   ├── signalindexcache.py               # EXISTING: Shared
│   ├── compactmeasurement.py             # EXISTING: Shared
│   ├── measurement.py                    # EXISTING: Shared
│   └── constants.py                      # EXISTING: Shared
└── data/                                 # EXISTING: All shared
```

## Testing Strategy

1. **Unit tests**: Test individual components (routing, encoding)
2. **Integration tests**: Python pub → Python sub loopback
3. **Interop tests**: Python pub → C++ sub (manual verification)
4. **Wire validation**: Capture and compare packets with C++ publisher

## Known Limitations

- Initial implementation focuses on TCP command channel (UDP data channel deferred)
- Temporal subscriptions supported but not deeply tested initially
- Reverse connections (publisher connecting to subscriber) implemented for parity but examples focus on normal mode

---

**Last Updated**: 2026-01-05
**Ported By**: GitHub Copilot
**C++ Reference Version**: As of cppapi commit at time of port
