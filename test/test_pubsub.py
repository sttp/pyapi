#!/usr/bin/env python3
# ******************************************************************************************************
#  test_pubsub.py - Pytest test for Publisher/Subscriber interoperability
#
#  Copyright © 2026, Grid Protection Alliance.  All Rights Reserved.
#
#  Licensed to the Grid Protection Alliance (GPA) under one or more contributor license agreements.
# ******************************************************************************************************

import sys
import os
import time
import threading
from random import random

# Add src to path (parent directory)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from sttp.publisher import Publisher
from sttp.subscriber import Subscriber
from sttp.transport.measurement import Measurement
from sttp.transport.constants import ServerCommand, ServerResponse
from sttp.data.dataset import DataSet
from sttp.ticks import Ticks
import numpy as np


def test_publisher_subscriber_interoperability():
    """Test Python publisher with Python subscriber."""
    # Local state for this test
    measurements_received = []
    received_count = [0]  # Use list to allow modification in nested function
    publish_count = [0]
    
    # Load metadata (from examples directory in parent folder)
    metadata_path = os.path.join(os.path.dirname(__file__), 
                                  '..', 'examples', 'simplepublish', 'Metadata.xml')
    with open(metadata_path, 'r') as f:
        metadata_xml = f.read()
    
    metadata, err = DataSet.from_xml(metadata_xml)
    assert not err, f"Failed to load metadata: {err}"
    
    # Create publisher
    publisher = Publisher()
    
    # Configure publisher callbacks
    def pub_status(msg):
        print(f"[PUB] {msg}")
    
    def pub_error(msg):
        print(f"[PUB ERROR] {msg}", file=sys.stderr)
    
    def pub_client_connected(conn):
        print(f"[PUB] Client connected: {conn.connection_id}")
    
    def pub_client_disconnected(conn):
        print(f"[PUB] Client disconnected: {conn.connection_id}")
    
    publisher.statusmessage_logger = pub_status
    publisher.errormessage_logger = pub_error
    publisher.clientconnected_receiver = pub_client_connected
    publisher.clientdisconnected_receiver = pub_client_disconnected
    
    # Start publisher
    port = 7166
    publisher.start(port)
    
    # Define metadata
    publisher.define_metadata(metadata)
    
    # Filter measurements to publish
    measurements_to_publish = publisher.filter_metadata("SignalAcronym <> 'STAT'")
    assert len(measurements_to_publish) > 0, "No measurements found for publication"
    
    # Create subscriber
    subscriber = Subscriber()
    
    # Configure subscriber callbacks
    def sub_status(msg):
        print(f"[SUB] {msg}")
    
    def sub_error(msg):
        print(f"[SUB ERROR] {msg}", file=sys.stderr)
    
    def sub_data_start(timestamp):
        print(f"[SUB] Data start time: {timestamp}")
    
    def sub_new_measurements(measurements):
        received_count[0] += len(measurements)
        measurements_received.extend(measurements)
    
    def sub_connected():
        print(f"[SUB] Connection established")
    
    def sub_disconnected():
        print(f"[SUB] Connection terminated")
    
    subscriber.statusmessage_logger = sub_status
    subscriber.errormessage_logger = sub_error
    subscriber.data_starttime_receiver = sub_data_start
    subscriber.newmeasurements_receiver = sub_new_measurements
    subscriber.connectionestablished_receiver = sub_connected
    subscriber.connectionterminated_receiver = sub_disconnected
    
    # Subscribe to measurements
    subscriber.subscribe("FILTER TOP 20 ActiveMeasurements WHERE SignalType <> 'STAT'")
    
    # Connect to publisher
    time.sleep(0.5)  # Give publisher time to start
    subscriber.connect(f"localhost:{port}")
    
    # Give connection time to establish
    time.sleep(1.0)
    
    # Publish measurements
    def publish_data():
        for round in range(10):
            timestamp = Ticks.utcnow()
            measurements = []
            
            for metadata in measurements_to_publish:
                measurement = Measurement()
                measurement.signalid = metadata.signalid
                measurement.timestamp = timestamp
                measurement.value = np.float64(random())
                measurements.append(measurement)
            
            publisher.publish_measurements(measurements)
            publish_count[0] += len(measurements)
            
            time.sleep(0.1)
    
    # Start publishing in background
    publish_thread = threading.Thread(target=publish_data)
    publish_thread.daemon = True
    publish_thread.start()
    
    # Wait for publishing to complete
    publish_thread.join()
    
    # Give subscriber time to receive final measurements
    time.sleep(1.0)
    
    # Cleanup
    subscriber.dispose()
    publisher.stop()
    
    # Verify results
    assert publish_count[0] > 0, "No measurements were published"
    assert received_count[0] > 0, f"No measurements received (published {publish_count[0]})"
    assert len(measurements_received) == received_count[0], "Measurement count mismatch"
    
    # Verify measurement structure
    assert measurements_received[0].signalid is not None, "First measurement missing signalid"
    assert measurements_received[0].value is not None, "First measurement missing value"
    assert measurements_received[0].timestamp > 0, "First measurement has invalid timestamp"

def test_usercommand_and_response():
    """Test user-defined commands and responses between publisher and subscriber."""
    # Shared state for tracking commands and responses
    commands_received = []
    responses_received = []
    
    # Load metadata
    metadata_path = os.path.join(os.path.dirname(__file__), 
                                  '..', 'examples', 'simplepublish', 'Metadata.xml')
    with open(metadata_path, 'r') as f:
        metadata_xml = f.read()
    
    metadata, err = DataSet.from_xml(metadata_xml)
    assert not err, f"Failed to load metadata: {err}"
    
    # Create publisher
    publisher = Publisher()
    
    # Configure publisher callbacks
    def pub_status(msg):
        print(f"[PUB] {msg}")
    
    def pub_error(msg):
        print(f"[PUB ERROR] {msg}", file=sys.stderr)
    
    def pub_client_connected(conn):
        print(f"[PUB] Client connected: {conn.connection_id}")
    
    def pub_user_command(connection, command, data):
        """Handle user commands from subscriber."""
        print(f"[PUB] Received user command {command} with {len(data) if data else 0} bytes of data")
        commands_received.append({
            'connection': connection,
            'command': command,
            'data': data
        })
        
        # Send response back to subscriber using user response codes
        # Map command to corresponding response code
        response_code = ServerResponse.USERRESPONSE00 + (command - ServerCommand.USERCOMMAND00)
        response_data = b"Command processed successfully"
        publisher.send_userresponse(connection, response_code, command, response_data)
    
    publisher.statusmessage_logger = pub_status
    publisher.errormessage_logger = pub_error
    publisher.clientconnected_receiver = pub_client_connected
    publisher.usercommand_receiver = pub_user_command
    
    # Start publisher
    port = 7167  # Different port from other test
    publisher.start(port)
    publisher.define_metadata(metadata)
    
    # Create subscriber
    subscriber = Subscriber()
    
    # Configure subscriber callbacks
    def sub_status(msg):
        print(f"[SUB] {msg}")
    
    def sub_error(msg):
        print(f"[SUB ERROR] {msg}", file=sys.stderr)
    
    def sub_user_response(responsecode, commandcode, data):
        """Handle user responses from publisher."""
        print(f"[SUB] Received user response {responsecode} for command {commandcode} with {len(data) if data else 0} bytes of data")
        responses_received.append({
            'responsecode': responsecode,
            'commandcode': commandcode,
            'data': data
        })
    
    def sub_connected():
        print(f"[SUB] Connection established")
    
    subscriber.statusmessage_logger = sub_status
    subscriber.errormessage_logger = sub_error
    subscriber.userresponse_receiver = sub_user_response
    subscriber.connectionestablished_receiver = sub_connected
    
    # Connect to publisher
    time.sleep(0.5)  # Give publisher time to start
    subscriber.connect(f"localhost:{port}")
    
    # Give connection time to establish
    time.sleep(1.0)
    
    # Test sending user commands
    test_commands = [
        (ServerCommand.USERCOMMAND00, b"Test command 0"),
        (ServerCommand.USERCOMMAND01, b"Test command 1 with more data"),
        (ServerCommand.USERCOMMAND05, b"Test command 5"),
        (ServerCommand.USERCOMMAND15, None),  # Command with no data
    ]
    
    for command, data in test_commands:
        print(f"[TEST] Sending command {command} with data: {data}")
        subscriber.send_usercommand(command, data)
        time.sleep(0.2)  # Give time for command to be processed
    
    # Wait for all responses
    time.sleep(1.0)
    
    # Cleanup
    subscriber.dispose()
    publisher.stop()
    
    # Verify results
    assert len(commands_received) == len(test_commands), \
        f"Expected {len(test_commands)} commands, received {len(commands_received)}"
    
    assert len(responses_received) == len(test_commands), \
        f"Expected {len(test_commands)} responses, received {len(responses_received)}"
    
    # Verify each command was received correctly
    for i, (expected_cmd, expected_data) in enumerate(test_commands):
        received = commands_received[i]
        assert received['command'] == expected_cmd, \
            f"Command {i}: expected {expected_cmd}, got {received['command']}"
        # None and empty bytes are equivalent in the protocol
        received_data = received['data']
        if expected_data is None:
            assert received_data == b'' or received_data is None, \
                f"Command {i}: expected None or b'', got {received_data}"
        else:
            assert received_data == expected_data, \
                f"Command {i}: expected {expected_data}, got {received_data}"
    
    # Verify each response was received correctly
    for i, (expected_cmd, _) in enumerate(test_commands):
        received = responses_received[i]
        # Response code should correspond to the command
        expected_response = ServerResponse.USERRESPONSE00 + (expected_cmd - ServerCommand.USERCOMMAND00)
        assert received['responsecode'] == expected_response, \
            f"Response {i}: expected {expected_response}, got {received['responsecode']}"
        assert received['commandcode'] == expected_cmd, \
            f"Response {i}: expected command {expected_cmd}, got {received['commandcode']}"
        assert received['data'] == b"Command processed successfully", \
            f"Response {i}: response data mismatch"


def test_broadcast_userresponse():
    """Test broadcasting user responses to multiple subscribers."""
    # Shared state
    subscriber1_responses = []
    subscriber2_responses = []
    
    # Load metadata
    metadata_path = os.path.join(os.path.dirname(__file__), 
                                  '..', 'examples', 'simplepublish', 'Metadata.xml')
    with open(metadata_path, 'r') as f:
        metadata_xml = f.read()
    
    metadata, err = DataSet.from_xml(metadata_xml)
    assert not err, f"Failed to load metadata: {err}"
    
    # Create publisher
    publisher = Publisher()
    
    def pub_status(msg):
        print(f"[PUB] {msg}")
    
    def pub_client_connected(conn):
        print(f"[PUB] Client connected: {conn.connection_id}")
        # After second client connects, broadcast a message
        if len(publisher.subscriber_connections) == 2:
            threading.Timer(0.5, lambda: publisher.broadcast_userresponse(
                ServerResponse.USERRESPONSE00,
                ServerCommand.USERCOMMAND00,
                b"Broadcast message to all clients"
            )).start()
    
    publisher.statusmessage_logger = pub_status
    publisher.clientconnected_receiver = pub_client_connected
    
    # Start publisher
    port = 7168  # Different port from other tests
    publisher.start(port)
    publisher.define_metadata(metadata)
    
    # Create first subscriber
    subscriber1 = Subscriber()
    
    def sub1_response(responsecode, commandcode, data):
        print(f"[SUB1] Received response {responsecode}")
        subscriber1_responses.append({
            'responsecode': responsecode,
            'commandcode': commandcode,
            'data': data
        })
    
    subscriber1.statusmessage_logger = lambda msg: print(f"[SUB1] {msg}")
    subscriber1.userresponse_receiver = sub1_response
    
    # Create second subscriber
    subscriber2 = Subscriber()
    
    def sub2_response(responsecode, commandcode, data):
        print(f"[SUB2] Received response {responsecode}")
        subscriber2_responses.append({
            'responsecode': responsecode,
            'commandcode': commandcode,
            'data': data
        })
    
    subscriber2.statusmessage_logger = lambda msg: print(f"[SUB2] {msg}")
    subscriber2.userresponse_receiver = sub2_response
    
    # Connect both subscribers
    time.sleep(0.5)
    subscriber1.connect(f"localhost:{port}")
    time.sleep(0.5)
    subscriber2.connect(f"localhost:{port}")
    
    # Wait for broadcast and responses
    time.sleep(2.0)
    
    # Cleanup
    subscriber1.dispose()
    subscriber2.dispose()
    publisher.stop()
    
    # Verify both subscribers received the broadcast
    assert len(subscriber1_responses) == 1, \
        f"Subscriber 1 should have received 1 response, got {len(subscriber1_responses)}"
    assert len(subscriber2_responses) == 1, \
        f"Subscriber 2 should have received 1 response, got {len(subscriber2_responses)}"
    
    # Verify response content
    for i, responses in enumerate([subscriber1_responses, subscriber2_responses], 1):
        response = responses[0]
        assert response['responsecode'] == ServerResponse.USERRESPONSE00, \
            f"Subscriber {i}: wrong response code"
        assert response['commandcode'] == ServerCommand.USERCOMMAND00, \
            f"Subscriber {i}: wrong command code"
        assert response['data'] == b"Broadcast message to all clients", \
            f"Subscriber {i}: wrong response data"


def test_invalid_usercommand():
    """Test that invalid user commands are rejected."""
    subscriber = Subscriber()
    
    # Try to send invalid command codes (not in USERCOMMAND00-15 range)
    try:
        subscriber.send_usercommand(ServerCommand.SUBSCRIBE, b"test")
        assert False, "Should have raised ValueError for invalid command"
    except ValueError as e:
        assert "not a valid user-defined command" in str(e)
        print(f"[TEST] Correctly rejected invalid command: {e}")
    
    subscriber.dispose()


if __name__ == "__main__":
    # Allow running directly for manual testing
    import pytest
    sys.exit(pytest.main([__file__, "-v"]))
