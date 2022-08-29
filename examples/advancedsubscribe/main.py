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
#  08/24/2022 - J. Ritchie Carroll
#       Generated original version of source code.
#
# ******************************************************************************************************

import os  # nopep8
import sys
sys.path.append(f"{os.path.dirname(os.path.realpath(__file__))}/../../src")  # nopep8

from gsf import Limits
from sttp.subscriber import Subscriber
from sttp.config import Config
from sttp.settings import Settings
from sttp.ticks import Ticks
from sttp.transport.measurement import Measurement
from sttp.transport.signalindexcache import SignalIndexCache
from typing import List
from time import time
import argparse

MAXPORT = Limits.MAXUINT16


class AdvancedSubscriber(Subscriber):
    def __init__(self):
        super().__init__()

        self.subscriber = Subscriber()
        self.config = Config()
        self.settings = Settings()
        self.lastmessage = 0.0

        self.subscriber.set_subscriptionupdated_receiver(self.subscription_updated)
        self.subscriber.set_newmeasurements_receiver(self.new_measurements)
        self.subscriber.set_connectionterminated_receiver(self.connection_terminated)

    def subscription_updated(self, signalindexcache: SignalIndexCache):
        self.statusmessage(f"Received signal index cache with {signalindexcache.count:,} mappings")

    def new_measurements(self, measurements: List[Measurement]):
        if time() - self.lastmessage < 5.0:
            return

        if self.lastmessage == 0.0:
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

        self.lastmessage = time()

    def connection_terminated(self):
        # Call default implementation which will display a connection terminated message to stderr
        self.default_connectionterminated_receiver()

        # Reset last message display time on disconnect
        self.lastmessage = 0.0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("hostname", type=str)
    parser.add_argument("port", type=int)
    args = parser.parse_args()

    if args.port < 1 or args.port > MAXPORT:
        print(f"Port number \"{args.port}\" is out of range: must be 1 to {MAXPORT}")
        exit(2)

    subscriber = AdvancedSubscriber()
    subscriber.config.compress_payloaddata = False
    subscriber.settings.udpport = 9600
    subscriber.settings.use_millisecondresolution = True

    try:
        subscriber.subscribe("FILTER TOP 20 ActiveMeasurements WHERE True", subscriber.settings)
        subscriber.connect(f"{args.hostname}:{args.port}", subscriber.config)

        # Exit when enter key is pressed
        input()
    finally:
        subscriber.dispose()


if __name__ == "__main__":
    main()
