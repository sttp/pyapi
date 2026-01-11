# ******************************************************************************************************
#  main.py - Gbtc
#
#  Copyright © 2019, Grid Protection Alliance.  All Rights Reserved.
#
#  Licensed to the Grid Protection Alliance (GPA) under one or more contributor license agreements. See
#  the NOTICE file distributed with this work for additional information regarding copyright ownership.
#  The GPA licenses this file to you under the MIT License (MIT), the "License"; you may not use this
#  file except in compliance with the License. You may obtain a copy of the License at:
#
#      http://opensource.org/licenses/MIT
#
#  Unless agreed to in writing, the subject software distributed under the License is distributed on an
#  "AS-IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. Refer to the
#  License for the specific language governing permissions and limitations.
#
#  Code Modification History:
#  ----------------------------------------------------------------------------------------------------
#  01/30/2019 - J. Ritchie Carroll
#       Generated original version of source code.
#
# ******************************************************************************************************

import sys
import os
from random import random
from threading import Timer
import numpy as np

# Add parent directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../src')))

from sttp.metadata.record.measurement import SignalType, MeasurementRecord
from sttp.publisher import Publisher
from sttp.transport.measurement import Measurement
from sttp.transport.subscriberconnection import SubscriberConnection
from sttp.data.dataset import DataSet
from sttp.ticks import Ticks
from typing import List
from threading import Timer as ThreadTimer

publisher: Publisher | None = None
publish_timer: ThreadTimer | None = None
measurements_to_publish: List[MeasurementRecord] = []


def run_publisher(port: int) -> bool:
    """Initialize and start the publisher."""
    global publisher, publish_timer, measurements_to_publish
    
    error_message = ""
    running = False
    
    try:
        publisher = Publisher()
        publisher.start(port)
        running = True
    except Exception as ex:
        error_message = str(ex)
    
    if running:
        assert publisher is not None
        print(f"\nListening on port: {port}...\n")
        
        # Register callbacks
        publisher.clientconnected_receiver = display_client_connected
        publisher.clientdisconnected_receiver = display_client_disconnected
        publisher.statusmessage_logger = display_status_message
        publisher.errormessage_logger = display_error_message
        
        # Define metadata - use path relative to this script file
        metadata_path = os.path.join(os.path.dirname(__file__), "Metadata.xml")
        dataset, err = DataSet.from_xml(open(metadata_path).read())
        
        if err is not None:
            print(f"ERROR: Failed to load metadata: {err}")
            return False
        
        publisher.define_metadata(dataset)
        
        # Filter metadata for measurements to publish from MeasurementDetail table
        measurements_to_publish = publisher.filter_metadata("SignalAcronym <> 'STAT'")  # List[MeasurementRecord]
        
        print(f"Loaded {len(measurements_to_publish)} measurement metadata records for publication.\n")
        
        # Setup data publication timer - for this simple publishing sample we just
        # send random values every 33 milliseconds
        publish_count = [0]  # Use list to avoid global issues
        
        def publish_data():
            assert publisher is not None
            global publish_timer
            
            # If metadata can change, the following count should not be static:
            count = len(measurements_to_publish)
            timestamp = Ticks.utcnow()
            measurements = []
            
            # Create new measurement values for publication
            for i in range(count):
                metadata = measurements_to_publish[i]
                measurement = Measurement()
                
                measurement.signalid = metadata.signalid
                measurement.timestamp = timestamp
                
                # Generate realistic values based on signal type
                if metadata.signaltype == SignalType.FREQ:
                    # Frequency around 60 Hz with small random variation
                    measurement.value = np.float64(60.0 + (random() - 0.5) * 0.02)  # 59.99 to 60.01 Hz
                else:
                    # Other measurements get random values
                    measurement.value = np.float64(random())
                
                measurements.append(measurement)
            
            # Publish measurements
            publisher.publish_measurements(measurements)
            
            # Feedback diagnostic
            publish_count[0] += 1

            if publish_count[0] % 100 == 0:  # Print every 100 cycles
                print(f"Published cycle {publish_count[0]}: {len(measurements)} measurements")
            
            # Schedule next publication
            publish_timer = Timer(0.033, publish_data)
            publish_timer.daemon = True
            publish_timer.start()
        
        # Start data publication
        publish_timer = Timer(0.033, publish_data)
        publish_timer.daemon = True
        publish_timer.start()
    else:
        print(f"Failed to listen on port: {port}: {error_message}", file=sys.stderr)
    
    return running


def display_client_connected(connection: SubscriberConnection):
    """Callback for client connected event."""
    message = f">> New Client Connected:\n"
    message += f"   Subscriber ID: {connection.subscriber_id}\n"
    message += f"   Connection ID: {connection.connection_id}"
    
    print(f"{message}\n")


def display_client_disconnected(connection: SubscriberConnection):
    """Callback for client disconnected event."""
    message = f">> Client Disconnected:\n"
    message += f"   Subscriber ID: {connection.subscriber_id}\n"
    message += f"   Connection ID: {connection.connection_id}"
    
    print(f"{message}\n")


def display_status_message(message: str):
    """Callback which is called to display status messages from the publisher."""
    print(f"{message}\n")


def display_error_message(message: str):
    """Callback which is called to display error messages from the publisher."""
    print(f"{message}\n", file=sys.stderr)


# Sample application to demonstrate the most simple use of the publisher API.
#
# This application accepts the port of the publisher via command line argument,
# starts listening for subscriber connections, then displays summary information
# about the measurements it publishes. It provides fourteen measurements, i.e.,
# PPA:1 through PPA:14
#
# Measurements are transmitted via the TCP command channel.
def main():
    # Ensure that the necessary command line arguments are given
    if len(sys.argv) < 2:
        print("Usage:")
        print("    python main.py PORT")
        return 0
    
    # Get port
    try:
        port = int(sys.argv[1])
    except ValueError:
        print(f"ERROR: Invalid port number: {sys.argv[1]}")
        return 1
    
    # Run the publisher
    if run_publisher(port):
        # Wait until the user presses enter before quitting
        input()
        
        # Stop data publication
        if publish_timer is not None:
            publish_timer.cancel()
    
    print("Publisher stopped.")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
