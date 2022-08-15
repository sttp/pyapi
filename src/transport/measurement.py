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

from ..gsf import Empty
from ..ticks.ticks import Ticks
from .constants import StateFlags
from uuid import UUID
from datetime import datetime
import numpy as np


class Measurement:
    """
    Represents a basic unit of measured data for transmission or reception in the STTP API.
    """

    def __init__(self,
                 signalID: UUID = ...,
                 value: np.float64 = ...,
                 timestamp: np.uint64 = ...,
                 flags: StateFlags = ...
                 ):
        self.signalID = Empty.GUID if signalID is ... else signalID
        self.value = np.float64(np.NAN) if value is ... else value
        self.timestamp = Empty.TICKS if timestamp is ... else timestamp
        self.flags = StateFlags.Normal if flags is ... else flags

    @property
    def SignalID(self) -> UUID:
        """
        Gets measurement's globally unique identifier.
        """
        return self.signalID

    @SignalID.setter
    def SignalID(self, value: UUID):
        """
        Sets measurement's globally unique identifier.
        """
        self.signalID = value

    @property
    def Value(self) -> np.float64:
        """
        Gets instantaneous value of the measurement.
        """
        return self.value

    @Value.setter
    def Value(self, value: np.float64):
        """
        Sets instantaneous value of the measurement.
        """
        self.value = value

    @property
    def Timestamp(self) -> np.uint64:
        """
        Gets the time, in ticks, that this measurement was taken.
        """
        return self.timestamp

    @Timestamp.setter
    def Timestamp(self, value: np.uint64):
        """
        Sets the time, in ticks, that this measurement was taken.
        """
        self.timestamp = value

    @property
    def Flags(self) -> StateFlags:
        """
        Gets flags indicating the state of the measurement as reported by the device that took it.
        """
        return self.flags

    @Flags.setter
    def Flags(self, value: StateFlags):
        """
        Sets flags indicating the state of the measurement as reported by the device that took it.
        """
        self.flags = value

    def TicksValue(self) -> np.int64:
        """
        Gets the integer-based time from a Measurement Ticks based timestamp, i.e.,
        the 62-bit time value excluding any reserved flags.
        """
        return self.Timestamp & Ticks.VALUEMASK

    def DateTime(self) -> datetime:
        """
        Gets a Measurement Ticks based timestamp as a standard Python datetime value.
        """
        return Ticks.ToDateTime(self.timestamp)

    def __repr__(self):
        return f"{self.SignalID} @ {Ticks.ToShortString(self.Timestamp)} = {self.Value:.3f} ({str(self.Flags).split('.')[1]})"
