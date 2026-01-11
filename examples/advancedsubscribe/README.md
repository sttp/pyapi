# Advanced Subscriber Example

This example demonstrates advanced STTP subscriber functionality with detailed measurement metadata display.

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
2. Subscribe to the top 20 non-statistical measurements using filter: `FILTER TOP 20 ActiveMeasurements WHERE SignalType <> 'STAT'`
3. Display signal index cache mapping count
4. Print detailed measurement information every 5 seconds including measurement IDs and signal IDs
5. Use millisecond timestamp resolution

## Configuration

This example configures:
- UDP port 9600 for data channel
- Millisecond timestamp resolution
- Payload data compression disabled

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
- "Received signal index cache with X mappings" message
- "Receiving measurements..." when data starts arriving
- Periodic detailed updates showing:
  - Total measurements received
  - Current timestamp
  - Table with measurement ID, Signal ID, and Value columns

Example output:
```
2,700 measurements received so far...
Timestamp: 2025-01-07 12:34:56.789
	ID	Signal ID				Value
	1	a1b2c3d4-e5f6-7890-abcd-ef1234567890	42.5
	2	b2c3d4e5-f6a7-8901-bcde-f12345678901	60.0
```

## Stopping the Subscriber

Press `Enter` to gracefully disconnect and shut down the subscriber.
