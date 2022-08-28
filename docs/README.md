## Python STTP ([IEEE 2664](https://standards.ieee.org/project/2664.html)) Implementation
### Streaming Telemetry Transport Protocol

<!--- Do not make this image location relative, README.md in root is a symbolic reference to one in docs. See CreateReadMeSymLink.cmd for more information. --->
<img align="right" src="https://raw.githubusercontent.com/sttp/pyapi/main/docs/img/sttp.png">

[![CodeQL](https://github.com/sttp/pyapi/actions/workflows/codeql-analysis.yml/badge.svg)](https://github.com/sttp/pyapi/actions/workflows/codeql-analysis.yml) [![docs](https://badgen.net/badge/sttpapi/python-reference/blue?icon=pypi)]( https://sttp.github.io/pyapi/html)

The Streaming Telemetry Transport Protocol (STTP) protocol is optimized for the demands of transporting high volume streaming data. The protocol allows for the transmission of any information that can be represented longitudinally, e.g., time-series data.

STTP is currently undergoing IEEE standardization (P2664), see: https://standards.ieee.org/ieee/2664/7397/

## Example Usage
```python
from sttp.subscriber import Subscriber
from time import time
from threading import Thread

def main():
    subscriber = Subscriber()

    try:
        # Start new data read at each connection
        subscriber.set_connectionestablished_receiver(
            lambda: Thread(target=read_data, args=(subscriber,)).start())

        subscriber.subscribe("FILTER TOP 20 ActiveMeasurements WHERE True")
        subscriber.connect("localhost:7175")

        # Exit when enter key is pressed
        input()
    finally:
        subscriber.dispose()


def read_data(subscriber: Subscriber):
    subscriber.default_connectionestablished_receiver()
    reader = subscriber.read_measurements()
    lastmessage = 0.0

    while subscriber.connected:
        (measurement, success) = reader.next_measurement()

        if not success:
            break

        if time() - lastmessage < 5.0:
            continue
        elif lastmessage == 0.0:
            subscriber.statusmessage("Receiving measurements...")
            lastmessage = time()
            continue

        message = []

        message.append(f"{subscriber.total_measurementsreceived:,}")
        message.append(" measurements received so far. Current measurement:\n    ")
        message.append(str(measurement))

        subscriber.statusmessage("".join(message))
        lastmessage = time()
```

Example Output:
```cmd
Connection to 127.0.0.1:7175 established.
Received 10,742 bytes of metadata in 0.045 seconds. Decompressing...
Decompressed 89,963 bytes of metadata in 0.004 seconds. Parsing...
Parsed 179 metadata records in 0.215 seconds
    Discovered:
        1 DeviceDetail records
        172 MeasurementDetail records
        5 PhasorDetail records
        1 SchemaVersion records
Metadata schema version: 14
Received success code in response to server command: Subscribe
Client subscribed as compact with 20 signals.
Receiving measurements...
1,470 measurements received so far. Current measurement:
    28bbb1fc-3434-48d3-87a8-bf5024c089d5 @ 19:43:53.600 = 516.545 (Normal)
2,970 measurements received so far. Current measurement:
    ed6def67-54c4-4e74-af95-c95fa6915fbc @ 19:43:58.600 = 218.070 (Normal)
4,460 measurements received so far. Current measurement:
    7aaf0a8f-3a4f-4c43-ab43-ed9d1e64a255 @ 19:44:03.633 = -0.230 (Normal)
5,930 measurements received so far. Current measurement:
    7aaf0a8f-3a4f-4c43-ab43-ed9d1e64a255 @ 19:44:08.633 = 8228.000 (Normal)

Connection to 127.0.0.1:7175 terminated.
```

## Links

* [STTP Python Documentation](https://sttp.github.io/pyapi/)
* [STTP Python API Reference](https://sttp.github.io/pyapi/html)
* [STTP General Documentation](https://sttp.github.io/documentation/)
* [STTP (IEEE 2664) Standard](https://standards.ieee.org/project/2664.html)


![Lock](https://raw.githubusercontent.com/sttp/pyapi/main/docs/img/LockPython_64High.png)