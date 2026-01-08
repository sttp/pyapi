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
import sys  # nopep8
sys.path.append(f"{os.path.dirname(os.path.realpath(__file__))}/../../src")  # nopep8

from gsf import Limits
from sttp.subscriber import Subscriber
from time import time
from threading import Thread
import argparse

MAXPORT = Limits.MAXUINT16


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("hostname", type=str)
    parser.add_argument("port", type=int)
    args = parser.parse_args()

    if args.port < 1 or args.port > MAXPORT:
        print(f"Port number \"{args.port}\" is out of range: must be 1 to {MAXPORT}")
        exit(2)

    subscriber = Subscriber()

    try:
        # Start new data read at each connection
        subscriber.connectionestablished_receiver = (
            lambda: Thread(target=read_data, args=(subscriber,), name="ReadDataThread").start())

        subscriber.subscribe("FILTER TOP 20 ActiveMeasurements WHERE SignalType <> 'STAT'")
        subscriber.connect(f"{args.hostname}:{args.port}")

        # Exit when enter key is pressed
        input()
    finally:
        subscriber.dispose()


def read_data(subscriber: Subscriber):
    subscriber.default_connectionestablished_receiver()
    subscriber.statusmessage("Measurement reader established")

    reader = subscriber.read_measurements()
    lastmessage = 0.0

    while subscriber.connected:
        measurement, success = reader.next_measurement()

        if not success:
            break

        if time() - lastmessage < 5.0:
            continue
        elif lastmessage == 0.0:
            subscriber.statusmessage("Receiving measurements...")
            lastmessage = time()
            continue

        message = [
            f"{subscriber.total_measurementsreceived:,}",
            " measurements received so far. Current measurement:\n    ",
            str(subscriber.measurement_metadata(measurement).pointtag),
            " -> ",
            str(measurement),
        ]

        subscriber.statusmessage("".join(message))
        lastmessage = time()

    subscriber.statusmessage("Measurement reader terminated")


if __name__ == "__main__":
    main()
