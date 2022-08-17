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

from gsf import Empty
from ticks import Ticks
from constants import StateFlags
from uuid import UUID
from datetime import datetime
import numpy as np


class Measurement:
    """
    Represents a basic unit of measured data for transmission or reception in the STTP API.
    """

    def __init__(self,
                 signalid: UUID = ...,
                 value: np.float64 = ...,
                 timestamp: np.uint64 = ...,
                 flags: StateFlags = ...
                 ):

        self.signalid = Empty.GUID if signalid is ... else signalid
        """
        Defines measurement's globally unique identifier.
        """

        self.value = np.float64(np.NAN) if value is ... else value
        """
        Defines instantaneous value of the measurement.
        """

        self.timestamp = Empty.TICKS if timestamp is ... else timestamp
        """
        Defines the time, in ticks, that measurement was taken.
        """

        self.flags = StateFlags.NORMAL if flags is ... else flags
        """
        Defines flags indicating the state of the measurement as reported by the device that took it.
        """

    def ticksvalue(self) -> np.int64:
        """
        Gets the integer-based time from a Measurement Ticks based timestamp, i.e.,
        the 62-bit time value excluding any reserved flags.
        """
        return self.timestamp & Ticks.VALUEMASK

    def datetime(self) -> datetime:
        """
        Gets a Measurement Ticks based timestamp as a standard Python datetime value.
        """
        return Ticks.todatetime(self.timestamp)

    def __repr__(self):
        return f"{self.signalid} @ {Ticks.toshortstring(self.timestamp)} = {self.value:.3f} ({str(self.flags).split('.')[1]})"
