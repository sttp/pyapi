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


if __name__ == "__main__":
    # Allow running directly for manual testing
    import pytest
    sys.exit(pytest.main([__file__, "-v"]))
