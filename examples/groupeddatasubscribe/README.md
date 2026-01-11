# Grouped Data Subscriber Example

This example demonstrates advanced STTP subscriber functionality with timestamp-aligned data grouping and processing.

## Usage

```bash
python main.py <hostname> <port>
```

## Example

```bash
# Connect to a publisher running on localhost port 7165
python main.py localhost 7165
```

The subscriber will:
1. Connect to the specified STTP publisher
2. Subscribe to frequency measurements using filter: `FILTER ActiveMeasurements WHERE SignalType = 'FREQ'`
3. Group incoming measurements by timestamp to the nearest second
4. Align measurements to subsecond distribution based on 30 samples per second
5. Process grouped data in one-second windows
6. Calculate and display average frequency for each second of data

## Key Features

- **Timestamp Grouping**: Groups measurements by whole seconds with subsecond alignment
- **Data Buffering**: Maintains 5-second measurement windows for processing
- **Time Validation**: Filters data based on configurable lag time (2 seconds) and lead time (2 seconds)
- **Downsampling Detection**: Tracks and reports when data rate exceeds processing capacity
- **Thread-Safe Processing**: Uses locks to ensure data integrity in multi-threaded environment

## Configuration

This example is configured with:
- Measurement window size: 5 seconds
- Samples per second: 30
- Lag time: 2.0 seconds
- Lead time: 2.0 seconds

## Testing with Python Publisher

In another terminal, you can test the subscriber using the Python publisher:

```bash
cd ../simplepublish
python main.py 7165
```

Note: The simple publisher may not generate frequency measurements. For best results, use a publisher that provides frequency data.

## Testing with C++ Publisher

You can also test interoperability with the C++ publisher:

```bash
cd ../../../cppapi/build
./SimplePublish 7165
```

## What to Expect

When connected to a publisher, you should see:
- "Measurement reader established" message
- "Receiving measurements..." when data starts arriving
- Average frequency calculations for each second of data
- Downsampling warnings if data rate is too high

Example output:
```
Receiving measurements...

Average frequency for 30 values in second 45: 60.001234 Hz

Average frequency for 28 values in second 46: 59.998765 Hz
   WARNING: 2 measurements downsampled in last measurement set...
```

## Data Processing

The example includes a `process_data` callback function that:
- Receives one-second buffers of time-aligned measurements
- Filters frequency values to reasonable range (59.95 to 60.05 Hz)
- Calculates average frequency across all valid measurements
- Demonstrates how to access measurement values, timestamps, and signal IDs
- Shows how to iterate through grouped measurement data

## Stopping the Subscriber

Press `Enter` to gracefully disconnect and shut down the subscriber.
