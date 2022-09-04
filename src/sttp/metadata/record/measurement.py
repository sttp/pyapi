# ******************************************************************************************************
#  metadata/record/measurement.py - Gbtc
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
# ******************************************************************************************************

from __future__ import annotations
from gsf import Empty
from typing import Optional, TYPE_CHECKING
from enum import IntEnum
from datetime import datetime
from uuid import UUID
import numpy as np

if TYPE_CHECKING:
    from .device import DeviceRecord
    from .phasor import PhasorRecord

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
    def parse(cls, name: str) -> SignalType:
        return getattr(cls, name.upper(), None)


class MeasurementRecord:
    """
    Represents a record of measurement metadata in the STTP.

    Note
    ----
    The `MeasurementRecord` defines  ancillary information associated with a `Measurement`.
    Metadata gets cached in a registry associated with a `DataSubscriber`.
    """

    DEFAULT_SIGNALID = Empty.GUID
    DEFAULT_ADDER = np.float64(0.0)
    DEFAULT_MULTIPLIER = np.float64(1.0)
    DEFAULT_ID = np.uint64(0)
    DEFAULT_SOURCE = Empty.STRING
    DEFAULT_SIGNALTYPENAME = "UNKN"
    DEFAULT_SIGNALREFERENCE = Empty.STRING
    DEFAULT_POINTTAG = Empty.STRING
    DEFAULT_DEVICEACRONYM = Empty.STRING
    DEFAULT_DESCRIPTION = Empty.STRING
    DEFAULT_UPDATEDON = Empty.DATETIME

    def __init__(self,
                 signalid: UUID,
                 adder: np.float64 = ...,
                 multiplier: np.float64 = ...,
                 id: np.uint64 = ...,
                 source: str = ...,
                 signaltypename: str = ...,
                 signalreference: str = ...,
                 pointtag: str = ...,
                 deviceacronym: str = ...,
                 description: str = ...,
                 updatedon: datetime = ...
                 ):
        """
        Constructs a new `MeasurementRecord`.
        """

        self._signalid = MeasurementRecord.DEFAULT_SIGNALID if signalid is ... else signalid
        self._adder = MeasurementRecord.DEFAULT_ADDER if adder is ... else adder
        self._multiplier = MeasurementRecord.DEFAULT_MULTIPLIER if multiplier is ... else multiplier
        self._id = MeasurementRecord.DEFAULT_ID if id is ... else id
        self._source = MeasurementRecord.DEFAULT_SOURCE if source is ... else source
        self._signaltypename = MeasurementRecord.DEFAULT_SIGNALTYPENAME if signaltypename is ... else signaltypename

        try:
            self._signaltype = SignalType.parse(self._signaltypename)
        except Exception:
            self._signaltype = SignalType.UNKN

        self._signalreference = MeasurementRecord.DEFAULT_SIGNALREFERENCE if signalreference is ... else signalreference
        self._pointtag = MeasurementRecord.DEFAULT_POINTTAG if pointtag is ... else pointtag
        self._deviceacronym = MeasurementRecord.DEFAULT_DEVICEACRONYM if deviceacronym is ... else deviceacronym
        self._description = MeasurementRecord.DEFAULT_DESCRIPTION if description is ... else description
        self._updatedOn = MeasurementRecord.DEFAULT_UPDATEDON if updatedon is ... else updatedon

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
    def signalid(self) -> UUID:  # <MeasurementDetail>/<SignalID>
        """
        Gets the unique guid-based signal identifier for this `MeasurementRecord`.
        """

        return self._signalid

    @property
    def adder(self) -> np.float64:  # <MeasurementDetail>/<Adder>
        """
        Gets the additive value modifier. Allows for linear value adjustment. Defaults to zero.
        """

        return self._adder

    @property
    def multiplier(self) -> np.float64:  # <MeasurementDetail>/<Multiplier>
        """
        Gets the multiplicative value modifier. Allows for linear value adjustment. Defaults to one.
        """

        return self._multiplier

    @property
    def id(self) -> np.uint64:  # <MeasurementDetail>/<ID> (right part of measurement key)
        """
        Gets the STTP numeric ID number (from measurement key) for this `MeasurementRecord`.
        """

        return self._id

    @property
    def source(self) -> str:  # <MeasurementDetail>/<ID> (left part of measurement key)
        """
        Gets the STTP source instance (from measurement key) for this `MeasurementRecord`.
        """

        return self._source

    @property
    def signaltypename(self) -> str:  # <MeasurementDetail>/<SignalAcronym>
        """
        Gets the signal type name for this `MeasurementRecord`, e.g., "FREQ".
        """

        return self._signaltypename

    @property
    def signaltype(self) -> SignalType:
        """
        Gets the `SignalType` enumeration for this `MeasurementRecord`, if it can
        be parsed from `signaltypename`; otherwise, returns `SignalType.UNKN`.
        """

        return self._signaltype

    @property
    def signalreference(self) -> str:  # <MeasurementDetail>/<SignalReference>
        """
        Gets the unique signal reference for this `MeasurementRecord`.
        """

        return self._signalreference

    @property
    def pointtag(self) -> str:  # <MeasurementDetail>/<PointTag>
        """
        Gets the unique point tag for this `MeasurementRecord`.
        """

        return self._pointtag

    @property
    def deviceacronym(self) -> str:  # <MeasurementDetail>/<DeviceAcronym>
        """
        Gets the alpha-numeric identifier of the associated device for this `MeasurementRecord`.
        """

        return self._deviceacronym

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
