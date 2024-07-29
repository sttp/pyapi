#******************************************************************************************************
#  measurement.py - Gbtc
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
#  08/14/2022 - J. Ritchie Carroll
#       Generated original version of source code.
#
#******************************************************************************************************

from gsf import Empty, normalize_enumname
from ..ticks import Ticks
from .constants import StateFlags
from uuid import UUID
from datetime import datetime
import numpy as np


class Measurement:
    """
    Represents a basic unit of measured data for transmission or reception in the STTP API.
    """

    DEFAULT_SIGNALID = Empty.GUID
    DEFAULT_VALUE = np.float64(np.nan)
    DEFAULT_TIMESTAMP = Empty.TICKS
    DEFAULT_FLAGS = StateFlags.NORMAL

    def __init__(self,
                 signalid: UUID = ...,
                 value: np.float64 = ...,
                 timestamp: np.uint64 = ...,
                 flags: StateFlags = ...
                 ):

        self.signalid = Measurement.DEFAULT_SIGNALID if signalid is ... else signalid
        """
        Defines measurement's globally unique identifier.
        """

        self.value = Measurement.DEFAULT_VALUE if value is ... else value
        """
        Defines instantaneous value of the measurement.
        """

        self.timestamp = Measurement.DEFAULT_TIMESTAMP if timestamp is ... else timestamp
        """
        Defines the STTP uint64 timestamp, in ticks, that measurement was taken.
        """

        self.flags = Measurement.DEFAULT_FLAGS if flags is ... else flags
        """
        Defines flags indicating the state of the measurement as reported by the device that took it.
        """

    @property
    def timestampvalue(self) -> np.int64:
        """
        Gets the integer-based time from a `Measurement` ticks-based timestamp, i.e.,
        the 62-bit time value excluding any leap-second flags.
        """
        return Ticks.timestampvalue(self.timestamp)

    @property
    def datetime(self) -> datetime:
        """
        Gets `Measurement` ticks-based timestamp as a standard Python datetime value.
        """
        return Ticks.to_datetime(self.timestamp)

    def __repr__(self):
        return f"{self.signalid} @ {Ticks.to_shortstring(self.timestamp)} = {self.value:.3f} ({normalize_enumname(self.flags)})"
