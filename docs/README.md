## Python STTP ([IEEE 2664](https://standards.ieee.org/project/2664.html)) Implementation
### Streaming Telemetry Transport Protocol

<!--- Do not make this image location relative, README.md in root is a symbolic reference to one in docs. See CreateReadMeSymLink.cmd for more information. --->
<img align="right" src="https://raw.githubusercontent.com/sttp/pyapi/main/docs/img/sttp.png">

[![CodeQL](https://github.com/sttp/pyapi/actions/workflows/github-code-scanning/codeql/badge.svg)](https://github.com/sttp/pyapi/actions/workflows/github-code-scanning/codeql) [![docs](https://raw.githubusercontent.com/sttp/pyapi/main/docs/img/py-ref.svg)]( https://sttp.github.io/pyapi) [![Release](https://img.shields.io/github/release/sttp/pyapi.svg?style=flat-square)](https://github.com/sttp/pyapi/releases/latest)

The Streaming Telemetry Transport Protocol (STTP) is optimized for the demands of transporting high volume streaming data. The protocol allows for the transmission of any information that can be represented longitudinally, e.g., time-series data.

STTP is an officially approved IEEE standard (2664), see: https://standards.ieee.org/ieee/2664/7397/

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
        measurement, success = reader.next_measurement()

        if not success:
            break

        if time() - lastmessage < 5.0:
            continue
        elif lastmessage == 0.0:
            subscriber.statusmessage("Receiving measurements...")
            lastmessage = time()
            continue

        message = [
            f"{subscriber.total_measurementsreceived:,}",
            " measurements received so far. Current measurement:\n    ",
            str(measurement)
        ]

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

### Publisher Example
```python
from sttp.publisher import Publisher
from sttp.transport.measurement import Measurement
from sttp.data.dataset import DataSet
from sttp.ticks import Ticks
from threading import Timer
import numpy as np
import os

def main():
    publisher = Publisher()
    
    try:
        # Start publisher on port 7165
        publisher.start(7165)
        
        # Load and define metadata
        metadata_path = os.path.join(os.path.dirname(__file__), "Metadata.xml")
        dataset, err = DataSet.from_xml(open(metadata_path).read())
        
        if err is not None:
            print(f"ERROR: Failed to load metadata: {err}")
            return
        
        publisher.define_metadata(dataset)
        
        # Get measurements to publish (exclude statistics)
        measurements_to_publish = publisher.filter_metadata("SignalAcronym <> 'STAT'")
        
        # Publish measurements every 33ms (30 Hz)
        def publish_data():
            timestamp = Ticks.utcnow()
            measurements = []
            
            for metadata in measurements_to_publish:
                measurement = Measurement()
                measurement.signalid = metadata.signalid
                measurement.timestamp = timestamp
                
                # Generate realistic values based on signal type
                if metadata.signalacronym == "FREQ":
                    measurement.value = np.float64(60.0 + (np.random.random() - 0.5) * 0.02)
                else:
                    measurement.value = np.float64(np.random.random())
                
                measurements.append(measurement)
            
            publisher.publish_measurements(measurements)
            
            # Schedule next publication
            Timer(0.033, publish_data).start()
        
        # Start publishing
        publish_data()
        
        # Wait for user input to stop
        input("Press Enter to stop publishing...\n")
        
    finally:
        publisher.stop()

if __name__ == "__main__":
    main()
```

## Examples
> [https://github.com/sttp/pyapi/tree/main/examples](https://github.com/sttp/pyapi/tree/main/examples)


## Support
For discussion and support, join our [discussions channel](https://github.com/sttp/pyapi/discussions) or [open an issue](https://github.com/sttp/pyapi/issues) on GitHub.

## Links

* [STTP PyPi Package: sttpapi](https://pypi.org/project/sttpapi/)
* [STTP Python Documentation](https://sttp.github.io/pyapi/)
* [STTP General Documentation](https://sttp.github.io/documentation/)
* [STTP (IEEE 2664) Standard](https://standards.ieee.org/ieee/2664/7397/)

[![Lock](https://raw.githubusercontent.com/sttp/pyapi/main/docs/img/LockPython_64High.png)](https://github.com/sttp/pyapi)
