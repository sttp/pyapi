#******************************************************************************************************
#  signalkind.py - Gbtc
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
#  08/17/2022 - J. Ritchie Carroll
#       Generated original version of source code.
#
#******************************************************************************************************

from enum import IntEnum


class SignalKind(IntEnum):
    """
    Enumeration of the possible kinds of signals a Measurement can represent.
    """

    ANGLE = 0
    """
    Angle defines a phase angle signal kind (could be a voltage or a current).
    """

    MAGNITUDE = 1
    """
    Magnitude defines a phase magnitude signal kind (could be a voltage or a current).
    """

    FREQUENCY = 2
    """
    Frequency defines a line frequency signal kind.
    """

    DFDT = 3
    """
    DfDt defines a frequency delta over time(dF/dt) signal kind.
    """

    STATUS = 4
    """
    Status defines a status flags signal kind.
    """

    DIGITAL = 5
    """
    Digital defines a digital value signal kind.
    """

    ANALOG = 6
    """
    Analog defines an analog value signal kind.
    """

    CALCULATION = 7
    """
    Calculation defines a calculated value signal kind.
    """

    STATISTIC = 8
    """
    Statistic defines a statistical value signal kind.
    """

    ALARM = 9
    """
    Alarm defines an alarm value signal kind.
    """

    QUALITY = 10
    """
    Quality defines a quality flags signal kind.
    """

    UNKNOWN = 11
    """
    Unknown defines an undetermined signal kind.
    """


class SignalKindEnum:
    """
    Helper functions for the `SignalKind` enumeration.
    """

    @staticmethod
    def acronym(signalkind: SignalKind) -> str:
        """
        Gets the `SignalKind` enumeration value as its two-character acronym string.
        """

        if signalkind < SignalKind.ANGLE or signalkind > SignalKind.UNKNOWN:
            signalkind = SignalKind.UNKNOWN

        return ["PA", "PM", "FQ", "DF", "SF", "DV", "AV", "CV", "ST", "AL", "QF", "??"][signalkind]

    @staticmethod
    def signaltype(signalkind: SignalKind, phasortype: str = ...) -> str:
        """
        Gets the specific four-character signal type acronym for a 'SignalKind'
        enumeration value and phasor type, i.e., "V" voltage or "I" current.

        Parameters
        ----------
        signalkind: The `SignalKind` enumeration value for the acronym.
        phasortype: "V" for voltage or "I" for current when `signalkind` is `SignalKind.ANGLE` or `SignalKind.MAGNITUDE`.
        """

        if signalkind < SignalKind.ANGLE or signalkind > SignalKind.UNKNOWN:
            signalkind = SignalKind.UNKNOWN

        phasortype = "?" if phasortype is ... else phasortype.upper()

        return [f"{phasortype}PHA", f"{phasortype}PHM", "FREQ", "DFDT", "FLAG", "DIGI", "ALOG", "CALC", "STAT", "ALRM", "QUAL", "NULL"][signalkind]

    @staticmethod
    def parse_acronym(acronym: str) -> SignalKind:  # sourcery skip: assign-if-exp, reintroduce-else
        """
        Gets the `SignalKind` enumeration value for the specified two-character acronym.
        """

        acronym = acronym.strip().upper()

        if acronym == "PA":  # Phase Angle
            return SignalKind.Angle

        if acronym == "PM":  # Phase Magnitude
            return SignalKind.Magnitude

        if acronym == "FQ":  # Frequency
            return SignalKind.Frequency

        if acronym == "DF":  # dF/dt
            return SignalKind.DfDt

        if acronym == "SF":  # Status Flags
            return SignalKind.Status

        if acronym == "DV":  # Digital Value
            return SignalKind.Digital

        if acronym == "AV":  # Analog Value
            return SignalKind.Analog

        if acronym == "CV":  # Calculated Value
            return SignalKind.Calculation

        if acronym == "ST":  # Statistical Value
            return SignalKind.Statistic

        if acronym == "AL":  # Alarm Value
            return SignalKind.Alarm

        if acronym == "QF":  # Quality Flags
            return SignalKind.Quality

        return SignalKind.Unknown
