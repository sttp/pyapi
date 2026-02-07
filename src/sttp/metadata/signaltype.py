# ******************************************************************************************************
#  metadata/signaltype.py - Gbtc
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

from enum import IntEnum
from sttp.metadata.signalreference import SignalKind


class SignalType(IntEnum):
    """
    Represents common signal types for STTP metadata. This list may
    not be exhaustive for some STTP deployments. If value is set to
    `UNKN`, check the string based `signalacronym` in the 
    `MeasurementRecord`.
    """

    IPHM = 1
    """Current phase magnitude"""
    IPHA = 2
    """Current phase angle"""
    VPHM = 3
    """Voltage phase magnitude"""
    VPHA = 4
    """Voltage phase angle"""
    FREQ = 5
    """Frequency"""
    DFDT = 6
    """Frequency derivative, i.e., Δfreq / Δtime"""
    ALOG = 7
    """Analog value (scalar)"""
    FLAG = 8
    """Status flags (16-bit)"""
    DIGI = 9
    """Digital value (16-bit)"""
    CALC = 10
    """Calculated value"""
    STAT = 11
    """Statistic value"""
    ALRM = 12
    """Alarm state"""
    QUAL = 13
    """Quality flags (16-bit)"""
    IPOW = 14
    """Point-on-wave current"""
    VPOW = 15
    """Point-on-wave voltage"""
    UNKN = -1
    """Unknown type, use `signalacronym` instead"""

    @property
    def acronym(self) -> str:
        """
        Gets the string representation of this `SignalType`.
        """
        return self.name

    @classmethod
    def parse(cls, name: str) -> SignalType:
        return getattr(cls, name.upper(), cls.UNKN)

    @property
    def signalkind(self) -> SignalKind:
        """
        Gets the associated `SignalKind` for this `SignalType`.
        """

        kind = _SIGNALTYPE_SIGNALKIND.get(self)
        return kind if kind is not None else SignalKind.UNKNOWN


_SIGNALTYPE_SIGNALKIND: dict[SignalType, SignalKind] = {
    SignalType.IPHM: SignalKind.MAGNITUDE,
    SignalType.IPHA: SignalKind.ANGLE,
    SignalType.VPHM: SignalKind.MAGNITUDE,
    SignalType.VPHA: SignalKind.ANGLE,
    SignalType.FREQ: SignalKind.FREQUENCY,
    SignalType.DFDT: SignalKind.DFDT,
    SignalType.ALOG: SignalKind.ANALOG,
    SignalType.FLAG: SignalKind.STATUS,
    SignalType.DIGI: SignalKind.DIGITAL,
    SignalType.CALC: SignalKind.CALCULATION,
    SignalType.STAT: SignalKind.STATISTIC,
    SignalType.ALRM: SignalKind.ALARM,
    SignalType.QUAL: SignalKind.QUALITY,
    SignalType.IPOW: SignalKind.POWCURRENT,
    SignalType.VPOW: SignalKind.POWVOLTAGE,
    SignalType.UNKN: SignalKind.UNKNOWN,
}