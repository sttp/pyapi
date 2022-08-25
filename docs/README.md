## Python STTP ([IEEE 2664](https://standards.ieee.org/project/2664.html)) Implementation
### Streaming Telemetry Transport Protocol

<!--- Do not make this image location relative, README.md in root is a symbolic reference to one in docs. See CreateReadMeSymLink.cmd for more information. --->
<img align="right" src="https://raw.githubusercontent.com/sttp/pyapi/main/docs/img/sttp.png">

[![CodeQL](https://github.com/sttp/pyapi/actions/workflows/codeql-analysis.yml/badge.svg)](https://github.com/sttp/pyapi/actions/workflows/codeql-analysis.yml)

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
            lambda: Thread(target=lambda: read_data(subscriber)).start())

        subscriber.subscribe("FILTER TOP 20 ActiveMeasurements WHERE True")
        subscriber.connect("localhost:7175")

        # Exit when enter key is pressed
        input()
    finally:
        subscriber.dispose()


def read_data(subscriber: Subscriber):
    reader = subscriber.readmeasurements()
    lastmessage = 0.0

    while subscriber.connected:
        measurement = reader.next_measurement()

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

## Links

* [STTP Python API Documentation](https://sttp.github.io/pyapi/)
* [STTP General Documentation](https://sttp.github.io/documentation/)
* [STTP (IEEE 2664) Standard](https://standards.ieee.org/project/2664.html)


![Lock](https://raw.githubusercontent.com/sttp/pyapi/main/docs/img/LockPython_64High.png)