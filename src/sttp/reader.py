# ******************************************************************************************************
#  reader.py - Gbtc
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

from .transport.measurement import Measurement
from typing import List
from queue import Queue

class MeasurementReader:
    """
    Defines an STTP measurement reader.
    """

    def __init__(self, subscriber: "Subscriber"):
        """
        Creates a new `MeasurementReader`.
        """

        self._queue = Queue(1)
        self._subscriber = subscriber
        self._subscriber.set_newmeasurements_receiver(self._read_measurements)

    def _read_measurements(self, measurements: List[Measurement]):
        for measurement in measurements:
            self._queue.put(measurement)
            self._queue.join()

    def next_measurement(self) -> Measurement:
        """
        Blocks current thread until a new measurement arrived.
        """

        current = self._queue.get()
        self._queue.task_done()
        return current
