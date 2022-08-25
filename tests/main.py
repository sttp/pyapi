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
#  08/12/2022 - J. Ritchie Carroll
#       Generated original version of source code.
#
# ******************************************************************************************************

import numpy as np
from time import time, sleep
from datetime import datetime, timedelta
from typing import Optional, List
from gsf import Ticks
from sttp.metadata.cache import MetadataCache
from sttp.metadata.record.measurement import SignalType
from tests.connection import Connection
import sys
import os

sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../src")


def main():
    print("Creating STTP API")

    sttpapi = Connection("localhost")

    print("Connecting...")

    try:
        sttpapi.Connect()

        if sttpapi.IsConnected:
            print(f"Connected to \"{sttpapi.HostAddress}\"!")

            # Suppress default output with: sttpapi.RefreshMetadata(logOutput = lambda value: None)
            recordCount = sttpapi.RefreshMetadata()

            # Get a reference to the STTP metadata cache
            metadata = sttpapi.Metadata

            # Lookup measurements for frequency signals
            records = metadata.measurements_with_signaltype(SignalType.FREQ)

            # Lookup measurements for voltage phase magnitudes
            #records.extend(metadata.GetMeasurementsBySignalType(SignalType.VPHM, instance.Name))

            # Lookup devices by matching text fields
            # for device in metadata.GetDevicesByTextSearch("WESTPNT"):
            #    records.extend(device.Measurements)

            recordCount = len(records)

            print(f"Queried {recordCount:,} metadata records.")
        else:
            print("Not connected? Unexpected.")
    except BaseException as ex:
        print(f"Failed to connect: {ex}")
    finally:
        if sttpapi.IsConnected:
            print("Disconnecting.")

        sttpapi.Disconnect()

if __name__ == "__main__":
    main()
