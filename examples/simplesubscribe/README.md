# Simple Subscriber Example

This example demonstrates basic STTP subscriber functionality.

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
2. Subscribe to the top 20 measurements using filter: `FILTER TOP 20 ActiveMeasurements WHERE True`
3. Display measurement statistics every 5 seconds
4. Print received measurement values with point tags

## Testing with Python Publisher

In another terminal, you can test the subscriber using the Python publisher:

```bash
cd ../simplepublish
python main.py 7165
```

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
- Periodic status updates showing total measurements received
- Current measurement display with point tag and value

Example output:
```
Receiving measurements...
2,700 measurements received so far. Current measurement:
    DEMO:POINT5 -> [2025-01-07 12:34:56.789] 42.5 (Normal)
```

## Stopping the Subscriber

Press `Enter` to gracefully disconnect and shut down the subscriber.
