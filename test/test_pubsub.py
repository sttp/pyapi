#!/usr/bin/env python3
# ******************************************************************************************************
#  test_pubsub.py - Test script for Publisher/Subscriber interoperability
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

# Global state
measurements_received = []
received_count = 0
publish_count = 0


def test_pubsub():
    """Test Python publisher with Python subscriber."""
    global measurements_received, received_count
    
    print("=" * 80)
    print("STTP Publisher/Subscriber Interoperability Test")
    print("=" * 80)
    print()
    
    # Load metadata (from examples directory in parent folder)
    metadata_path = os.path.join(os.path.dirname(__file__), 
                                  '..', 'examples', 'simplepublish', 'Metadata.xml')
    with open(metadata_path, 'r') as f:
        metadata_xml = f.read()
    
    metadata, err = DataSet.from_xml(metadata_xml)
    if err:
        print(f"ERROR: Failed to load metadata: {err}")
        return False
    
    # Create publisher
    print("Creating publisher...")
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
    try:
        publisher.start(port)
        print(f"[PUB] Listening on port {port}")
    except Exception as ex:
        print(f"ERROR: Failed to start publisher: {ex}")
        return False
    
    # Define metadata
    publisher.define_metadata(metadata)
    
    # Filter measurements to publish
    measurements_to_publish = publisher.filter_metadata("SignalAcronym <> 'STAT'")
    print(f"[PUB] Loaded {len(measurements_to_publish)} measurements for publication")
    print()
    
    # Create subscriber
    print("Creating subscriber...")
    subscriber = Subscriber()
    
    # Configure subscriber callbacks
    def sub_status(msg):
        print(f"[SUB] {msg}")
    
    def sub_error(msg):
        print(f"[SUB ERROR] {msg}", file=sys.stderr)
    
    def sub_data_start(timestamp):
        print(f"[SUB] Data start time: {timestamp}")
    
    def sub_new_measurements(measurements):
        global measurements_received, received_count
        received_count += len(measurements)
        measurements_received.extend(measurements)
        if received_count % 100 == 0:
            print(f"[SUB] Received {received_count} measurements")
    
    def sub_connected():
        print(f"[SUB] Connection established")
    
    def sub_disconnected():
        print(f"[SUB] Connection terminated")
    
    subscriber.set_statusmessage_logger(sub_status)
    subscriber.set_errormessage_logger(sub_error)
    subscriber.set_data_starttime_receiver(sub_data_start)
    subscriber.set_newmeasurements_receiver(sub_new_measurements)
    subscriber.set_connectionestablished_receiver(sub_connected)
    subscriber.set_connectionterminated_receiver(sub_disconnected)
    
    # Subscribe to measurements
    subscriber.subscribe("FILTER TOP 20 ActiveMeasurements WHERE SignalType <> 'STAT'")
    
    # Connect to publisher
    print(f"[SUB] Connecting to localhost:{port}...")
    time.sleep(0.5)  # Give publisher time to start
    
    try:
        subscriber.connect(f"localhost:{port}")
    except Exception as ex:
        print(f"ERROR: Failed to connect subscriber: {ex}")
        publisher.stop()
        return False
    
    print()
    
    # Give connection time to establish
    time.sleep(1.0)
    
    # Publish measurements
    print("Publishing measurements...")
    print()
    
    def publish_data():
        global publish_count
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
            publish_count += len(measurements)
            
            if (round + 1) % 2 == 0:
                print(f"[PUB] Published {publish_count} measurements")
            
            time.sleep(0.1)
    
    # Start publishing in background
    publish_thread = threading.Thread(target=publish_data)
    publish_thread.daemon = True
    publish_thread.start()
    
    # Wait for publishing to complete
    publish_thread.join()
    
    # Give subscriber time to receive final measurements
    time.sleep(1.0)
    
    # Display results
    print()
    print("=" * 80)
    print("Test Results:")
    print("=" * 80)
    print(f"Measurements published: {publish_count}")
    print(f"Measurements received:  {received_count}")
    
    if received_count > 0:
        print(f"\n✓ SUCCESS: Received {received_count} measurements")
        print(f"  First measurement: SignalID={measurements_received[0].signalid}, Value={measurements_received[0].value:.6f}")
        if len(measurements_received) > 1:
            print(f"  Last measurement:  SignalID={measurements_received[-1].signalid}, Value={measurements_received[-1].value:.6f}")
        success = True
    else:
        print(f"\n✗ FAILURE: No measurements received")
        success = False
    
    # Cleanup
    print()
    print("Cleaning up...")
    subscriber.dispose()
    publisher.stop()
    
    print()
    return success


if __name__ == "__main__":
    try:
        success = test_pubsub()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\nTest interrupted")
        sys.exit(1)
    except Exception as ex:
        print(f"\nTest failed with exception: {ex}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
