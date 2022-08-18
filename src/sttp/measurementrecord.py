#******************************************************************************************************
#  measurement_record.py - Gbtc
#
#  Copyright © 2021, Grid Protection Alliance.  All Rights Reserved.
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
#  02/07/2021 - J. Ritchie Carroll
#       Generated original version of source code.
#
#******************************************************************************************************

from devicerecord import DeviceRecord
from phasorrecord import PhasorRecord
from gsf import Empty
from typing import Optional
from enum import IntEnum
from datetime import datetime
from uuid import UUID
import numpy as np


class SignalType(IntEnum):
    """
    Represents common signal types for STTP metadata. This list may
    not be exhaustive for some STTP deployments. If value is set to
    `UNKN`, check the string based `SignalTypeName` in the `MeasurementRecord`.
    """
    IPHM = 1    # Current phase magnitude
    IPHA = 2    # Current phase angle
    VPHM = 3    # Voltage phase magnitude
    VPHA = 4    # Voltage phase angle
    FREQ = 5    # Frequency
    DFDT = 6    # Frequency derivative, i.e., Δfreq / Δtime
    ALOG = 7    # Analog value (scalar)
    FLAG = 8    # Status flags (16-bit)
    DIGI = 9    # Digital value (16-bit)
    CALC = 10   # Calculated value
    STAT = 11   # Statistic value
    ALRM = 12   # Alarm state
    QUAL = 13   # Quality flags (16-bit)
    UNKN = -1   # Unknown type, see `SignalTypeName`

    @classmethod
    def parse(cls, name):
        return getattr(cls, name.upper(), None)


class MeasurementRecord:
    """
    Represents a record of measurement metadata in the STTP.
    """

    def __init__(self,
                 instancename: str,
                 pointid: np.uint64,
                 signalid: UUID,
                 pointtag: str,
                 signalreference: str = "",
                 signaltypename: str = "UNKN",
                 deviceacronym: str = "",
                 description: str = "",
                 updatedon: datetime = Empty.DATETIME
                 ):
        """
        Constructs a new `MeasurementRecord`.
        """
        self._instancename = instancename
        self._pointid = pointid
        self._signalid = signalid
        self._pointtag = pointtag
        self._signalreference = signalreference
        self._signaltypename = signaltypename

        try:
            self._signaltype = SignalType.parse(self._signaltypename)
        except:
            self._signaltype = SignalType.UNKN

        self._deviceAcronym = deviceacronym
        self._description = description
        self._updatedOn = updatedon

        self.device: Optional[DeviceRecord] = None
        """
        Defines the associated `DeviceRecord` for this `MeasurementRecord`.
        Set to `None` if not applicable.
        """

        self.phasor: Optional[PhasorRecord] = None
        """
        Defines the associated `PhasorRecord` for this `MeasurementRecord`.
        Set to `None` if not applicable.
        """

    @property
    def instancename(self) -> str:  # <MeasurementDetail>/<ID> (left part of measurement key)
        """
        Gets the STTP client database instance for this `MeasurementRecord`.
        """
        return self._instancename

    @property
    def pointid(self) -> np.uint64:  # <MeasurementDetail>/<ID> (right part of measurement key)
        """
        Gets the STTP point ID for this `MeasurementRecord`.
        """
        return self._pointid

    @property
    def signalid(self) -> UUID:  # <MeasurementDetail>/<SignalID>
        """
        Gets the unique guid-based signal identifier for this `MeasurementRecord`.
        """
        return self._signalid

    @property
    def pointtag(self) -> str:  # <MeasurementDetail>/<PointTag>
        """
        Gets the unique point tag for this `MeasurementRecord`.
        """
        return self._pointtag

    @property
    def signalreference(self) -> str:  # <MeasurementDetail>/<SignalReference>
        """
        Gets the unique signal reference for this `MeasurementRecord`.
        """
        return self._signalreference

    @property
    def signaltypename(self) -> str:  # <MeasurementDetail>/<SignalAcronym>
        """
        Gets the signal type name for this `MeasurementRecord`.
        """
        return self._signaltypename

    @property
    def signaltype(self) -> SignalType:
        """
        Gets the `SignalType` enumeration for this `MeasurementRecord`, if it can be mapped
        to `SignalTypeName`; otherwise, returns `SignalType.UNKN`.
        """
        return self._signaltype

    @property
    def deviceacronym(self) -> str:  # <MeasurementDetail>/<DeviceAcronym>
        """
        Gets the alpha-numeric identifier of the associated device for this `MeasurementRecord`.
        """
        return self._deviceAcronym

    @property
    def description(self) -> str:  # <MeasurementDetail>/<Description>
        """
        Gets the description for this `MeasurementRecord`.
        """
        return self._description

    @property
    def updatedon(self) -> datetime:  # <MeasurementDetail>/<UpdatedOn>
        """
        Gets the `datetime` of when this `MeasurementRecord` was last updated.
        """
        return self._updatedOn
