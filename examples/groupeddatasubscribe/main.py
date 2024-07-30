# ******************************************************************************************************
#  main.py - Gbtc
#
#  Copyright © 2022, Grid Protection Alliance.  All Rights Reserved.
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
#  07/30/2024 - J. Ritchie Carroll
#       Generated original version of source code.
#
# ******************************************************************************************************

from __future__ import annotations
import os
import sys
import threading
import numpy as np
import argparse

sys.path.append(f"{os.path.dirname(os.path.realpath(__file__))}/../../src") 

from gsf import Limits
from sttp.subscriber import Subscriber
from sttp.config import Config
from sttp.settings import Settings
from sttp.ticks import Ticks
from sttp.transport.measurement import Measurement
from sttp.transport.signalindexcache import SignalIndexCache
from typing import Callable, List, Dict, Optional
from uuid import UUID
from time import time

MAXPORT = Limits.MAXUINT16

class GroupedDataSubscriber(Subscriber):
    """
    Example subscriber that groups data by timestamp to the nearest second and publishes grouped data after a specified
    measurement window size has elapsed. This subscriber demonstrates how to group data by timestamp and publish grouped
    data to a user defined callback function. Data is grouped by timestamp to the nearest second and then further grouped
    by subsecond distribution based on the number of samples per second. The grouped data is then published to a user
    defined callback function that handles the grouped data.

    If incoming frame rate is higher than the samples per second, or timestamp alignment does not accurately coinside with
    the subsecond distribution, some data will be downsampled. Downsampled data count is tracked and reported to through the
    downsampled_count property.

    Only a single one-second data buffer will be published at a time. If data cannot be processed within the one-second
    window, a warning message will be displayed and the data will be skipped. The number of skipped data sets is tracked
    and reported through the process_missed_count property.

    This example depends on a semi-accurate system clock to group data by timestamp. If the system clock is not accurate,
    data may not be grouped as expected.
    """

    def __init__(self):
        super().__init__()

        # Configure STTP subscriber settings
        self.config = Config()
        self.settings = Settings()

        self.measurement_window_size = 1
        """
        Defines measurement window size, in whole seconds, for data grouping.
        """

        self.lag_time = 5.0
        """
        Defines the lag time, in seconds, for data grouping. Data received outside
        of this past time limit, relative to local clock, will be discarded.
        """

        self.lead_time = 5.0
        """
        Defines the lead time, in seconds, for data grouping. Data received outside
        this future time limit, relative to local clock,  will be discarded.
        """

        self.samples_per_second = 30
        """
        Defines the number of samples per second for the data in the stream.
        """

        self.display_measurement_summary = False
        """
        Defines if the subscriber should display a summary of received measurements
        every few seconds.
        """
        
        self._grouped_data: Dict[np.uint64, Dict[UUID, Measurement]] = {} 
        self._grouped_data_receiver: Optional[Callable[[GroupedDataSubscriber, np.uint64, Dict[np.uint64, Dict[UUID, Measurement]]], None]] = None

        self._lastmessage = 0.0

        self._downsampled_count_lock = threading.Lock()
        self._downsampled_count = 0

        self._process_lock = threading.Lock()
        
        self._process_missed_count_lock = threading.Lock()
        self._process_missed_count = 0

        # Set up event handlers for STTP API
        self.set_subscriptionupdated_receiver(self._subscription_updated)
        self.set_newmeasurements_receiver(self._new_measurements)
        self.set_connectionterminated_receiver(self._connection_terminated)

    @property
    def downsampled_count(self) -> int:
        """
        Gets the count of downsampled measurements.
        """

        with self._downsampled_count_lock:
            return self._downsampled_count

    @downsampled_count.setter
    def downsampled_count(self, value: np.int32):
        """
        Sets the count of downsampled measurements.
        """

        with self._downsampled_count_lock:
            self._downsampled_count = value

    @property
    def process_missed_count(self) -> int:
        """
        Gets the count of missed data processing.
        """

        with self._process_missed_count_lock:
            return self._process_missed_count
        
    @process_missed_count.setter
    def process_missed_count(self, value: np.int32):
        """
        Sets the count of missed data processing.
        """

        with self._process_missed_count_lock:
            self._process_missed_count = value

    def set_grouped_data_receiver(self, callback: Optional[Callable[[GroupedDataSubscriber, np.uint64, Dict[np.uint64, Dict[UUID, Measurement]]], None]]):
        """
        Defines the callback function that handles grouped data that has been received.

        Function signature:
            def handle_data(GroupedDataSubscriber subscriber, timestamp: np.uint64, data_buffer: Dict[np.uint64, Dict[UUID, Measurement]]):
                pass
        """

        self._grouped_data_receiver = callback

    def _time_is_valid(self, timestamp: np.uint64) -> bool:
        """
        Determines if the given timestamp is within the valid time range for data grouping.
        """
        distance = Ticks.utcnow() - timestamp
        lead_time = self.lead_time * Ticks.PERSECOND
        lag_time = self.lag_time * Ticks.PERSECOND

        return distance >= -lead_time and distance <= lag_time

    def _round_to_nearest_second(self, timestamp: np.uint64) -> np.uint64:
        """
        Rounds the timestamp rounded to the nearest second.
        """
        
        return timestamp - timestamp % Ticks.PERSECOND

    def _round_to_subsecond_distribution(self, timestamp: np.uint64) -> np.uint64:
        """
        Rounds the timestamp to the nearest subsecond distribution based on the number of samples per second.
        """
       
        # Baseline timestamp to the top of the second
        base_ticks = self._round_to_nearest_second(timestamp)

        # Remove the seconds from ticks
        ticks_beyond_second = timestamp - base_ticks

        # Calculate a frame index between 0 and m_framesPerSecond - 1,
        # corresponding to ticks rounded to the nearest frame
        frame_index = np.round(ticks_beyond_second / (Ticks.PERSECOND / self.samples_per_second))

        # Calculate the timestamp of the nearest frame
        destination_ticks = np.uint64(frame_index * Ticks.PERSECOND / self.samples_per_second)

        # Recover the seconds that were removed
        destination_ticks += base_ticks

        return destination_ticks

    def _subscription_updated(self, signalindexcache: SignalIndexCache):
        self.statusmessage(f"Received signal index cache with {signalindexcache.count:,} mappings")

    def _new_measurements(self, measurements: List[Measurement]):
        # Collect data into a map grouped by timestamps to the nearest second
        for measurement in measurements:
            # Get timestamp rounded to the nearest second
            timestamp_second = self._round_to_nearest_second(measurement.timestamp)

            if self._time_is_valid(timestamp_second):
                # Create a new one-second timestamp map if it doesn't exist
                if timestamp_second not in self._grouped_data:
                    self._grouped_data[timestamp_second] = {}

                # Get timestamp rounded to the nearest subsecond distribution, e.g., 000, 033, 066, 100 ms
                timestamp_subsecond = self._round_to_subsecond_distribution(measurement.timestamp)

                # Create a new subsecond timestamp map if it doesn't exist                
                if timestamp_subsecond not in self._grouped_data[timestamp_second]:
                    self._grouped_data[timestamp_second][timestamp_subsecond] = {}

                # Append measurement to subsecond timestamp list, tracking downsampled measurements
                if measurement.signalid in self._grouped_data[timestamp_second][timestamp_subsecond]:
                    with self._downsampled_count_lock:
                        self._downsampled_count += 1

                self._grouped_data[timestamp_second][timestamp_subsecond][measurement.signalid] = measurement

        # Check if it's time to publish grouped data, waiting for measurement_window_size to elapse. Note
        # that this implementation depends on continuous data reception to trigger data publication. A more
        # robust implementation would use a precision timer to trigger data publication.
        current_time = Ticks.utcnow()
        window_size = np.uint64(self.measurement_window_size * Ticks.PERSECOND)

        for timestamp in list(self._grouped_data.keys()):
            if current_time - timestamp >= window_size:
                grouped_data = self._grouped_data.pop(timestamp)

                # Call user defined data function handler with one-second grouped data buffer on a separate thread
                threading.Thread(target=self._publish_data, args=(timestamp, grouped_data), name="PublishDataThread").start()
 
        # Provide user feedback on data reception
        if not self.display_measurement_summary or time() - self._lastmessage < 5.0:
            return

        try:
            if self._lastmessage == 0.0:
                self.statusmessage("Receiving measurements...")
                return

            message = [
                f"{self.total_measurementsreceived:,} measurements received so far...\n",
                f"Timestamp: {Ticks.to_string(measurements[0].timestamp)}\n",
                "\tID\tSignal ID\t\t\t\tValue\n"
            ]

            for measurement in measurements:
                metadata = self.measurement_metadata(measurement)
                message.append(f"\t{metadata.id}\t{measurement.signalid}\t{measurement.value:.6}\n")

            self.statusmessage("".join(message))
        finally:
            self._lastmessage = time()

    def _publish_data(self, timestamp: np.uint64, data_buffer: Dict[np.uint64, Dict[UUID, Measurement]]):
        data_buffer_time_str = Ticks.to_shortstring(timestamp).split(".")[0]

        if self._process_lock.acquire(False):
            try:
                process_started = time()

                if self._grouped_data_receiver is not None:
                    self._grouped_data_receiver(self, timestamp, data_buffer)

                self.statusmessage(f"Data publication for buffer at {data_buffer_time_str} processed in {self._get_elapsed_time_str(time() - process_started)}.")
            finally:
                self._process_lock.release()
        else:
            with self._process_missed_count_lock:
                self._process_missed_count += 1
                self.errormessage(f"WARNING: Data publication missed for buffer at {data_buffer_time_str}, a previous data buffer is still processing. {self._process_missed_count:,} data sets missed so far...")

    def _get_elapsed_time_str(self, elapsed: float) -> str:
        hours, rem = divmod(elapsed, 3600)
        minutes, seconds = divmod(rem, 60)
        milliseconds = (elapsed - int(elapsed)) * 1000

        if hours < 1.0:
            if minutes < 1.0:
                if seconds < 1.0:
                    return f"{int(milliseconds):03} ms"

                return f"{int(seconds):02}.{int(milliseconds):03} sec"
                        
            return f"{int(minutes):02}:{int(seconds):02}.{int(milliseconds):03}"
                        
        return f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}.{int(milliseconds):03}"

    def _connection_terminated(self):
        # Call default implementation which will display a connection terminated message to stderr
        self.default_connectionterminated_receiver()

        # Reset last message display time on disconnect
        self._lastmessage = 0.0

        # Reset grouped data on disconnect
        self.downsampled_count = 0

        # Reset process missed count on disconnect
        self.process_missed_count  = 0

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("hostname", type=str)
    parser.add_argument("port", type=int)
    args = parser.parse_args()

    if args.port < 1 or args.port > MAXPORT:
        print(f"Port number \"{args.port}\" is out of range: must be 1 to {MAXPORT}")
        exit(2)

    subscriber = GroupedDataSubscriber()
    
    # Set user defined callback function to handle grouped data:
    subscriber.set_grouped_data_receiver(process_data)

    try:
        subscriber.subscribe("FILTER ActiveMeasurements WHERE SignalType = 'FREQ'", subscriber.settings)
        subscriber.connect(f"{args.hostname}:{args.port}", subscriber.config)

        # Exit when enter key is pressed
        input()
    finally:
        subscriber.dispose()

def process_data(subscriber: GroupedDataSubscriber, timestamp: np.uint64, data_buffer: Dict[np.uint64, Dict[UUID, Measurement]]):
    """
    User defined callback function that handles grouped data that has been received.

    Note: This function is called by the subscriber when grouped data is available for processing.
    The function will only be called once per second with a buffer of grouped data for the second.
    If the function processing time exceeds the one second window, a warning message will be displayed
    and the data will be skipped. The number of skipped data sets is tracked and reported through the
    process_missed_count property.
    
    Parameters:
        timestamp:   The timestamp, at top of second, for the grouped data
        data_buffer: The grouped one second data buffer:
                     np.uint64: sub-second timestamps of aligned measurement groups
                     Dict[UUID, Measurement]: aligned measurements for the sub-second timestamp
    """

    # In this example, we calculate average frequency for all frequencies in the one second buffer
    frequency_sum = 0.0
    frequency_count = 0

    # Loop through each set of measurement groups in the one second buffer
    for measurements in data_buffer.values():
        # To use subsecond timestamp values, you can use the following loop instead:
        #     for subsecond_timestamp, measurements in data_buffer.items():

        # subsecond_timestamp is the timestamp rounded to the nearest subsecond distribution.
        # Milliseconds of the timestamp at 30 samples per second are 0, 33, 66, or 100 ms, etc.
        # For example:
        #    2024-07-30 17:55:29.233
        #    2024-07-30 17:55:29.266
        #    2024-07-30 17:55:29.333
        #    2024-07-30 17:55:29.366

        # At this point, all measurements are aligned to the same subsecond timestamp
        for measurement in measurements.values():
            # To use UUID values, you can use the following loop instead:
            #     for signalid, measurement in measurements.items():

            # If you know which measurement you are looking for, you can use the following loopup:
            #     measurement = measurements.get(my_signalid)

            # Note:
            #   measurement.value is a numpy float64
            #   measurement.timestamp is a numpy uint64 (in ticks, i.e., 100-nanosecond intervals)
            #    - use Ticks.to_string to convert to a human readable string
            #    - use Ticks.to_datetime to convert to a Python datetime
            #   measurement.signalid is a UUID
            #    - use str(measurement.signalid) to convert to a human readable string
            #    - use self.measurement_metadata to get associated MeasurementRecord
            #
            # See measurement.py for more details

            # Ensure frequency is in reasonable range (59.95 to 60.05 Hz) and not NaN
            if not np.isnan(measurement.value) and measurement.value >= 59.95 and measurement.value <= 60.05:
                # The following line demonstrates how to use the value of a measurement based on its
                # linear adjustment factor metadata , i.e., the configured adder and multiplier:
                #frequency_sum += subscriber.adjustedvalue(measurement)                
                frequency_sum += measurement.value # raw, unadjusted value
                frequency_count += 1

    average_frequency = frequency_sum / frequency_count

    subscriber.statusmessage(f"\nAverage frequency for {frequency_count:,} values in second {Ticks.to_datetime(timestamp).second}: {average_frequency:.6f} Hz")

    if subscriber.downsampled_count > 0:
        subscriber.statusmessage(f"   Downsampled {subscriber.downsampled_count:,} measurements in last measurement set...")
        subscriber.downsampled_count = 0

if __name__ == "__main__":
    main()
