# Simple Publisher Example

This example demonstrates basic STTP publisher functionality.

## Usage

```bash
python main.py <port>
```

## Example

```bash
# Start the publisher on port 7165
python main.py 7165
```

The publisher will:
1. Start listening on the specified port
2. Create sample metadata with 10 measurements
3. Publish randomly generated measurement values every 33ms (~30 Hz)
4. Accept connections from STTP subscribers

## Testing with Python Subscriber

In another terminal, you can test the publisher using the existing Python subscriber:

```bash
cd ../simplesubscribe
python main.py localhost 7165
```

## Testing with C++ Subscriber

You can also test interoperability with the C++ subscriber:

```bash
cd ../../../cppapi/build
./SimpleSubscribe localhost 7165
```

## What to Expect

When a subscriber connects, you should see:
- "Client connected: <ip>:<port>" message
- Measurement data being published regularly
- Any subscription/metadata requests being processed

The publisher creates 10 demo measurements with:
- Signal IDs: Sequential UUIDs
- Point Tags: DEMO:POINT1 through DEMO:POINT10
- Signal Type: CALC
- Random float values between 0-100

## Stopping the Publisher

Press `Ctrl+C` to gracefully shut down the publisher.
