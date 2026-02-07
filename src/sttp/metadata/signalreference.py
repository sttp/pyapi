# ******************************************************************************************************
#  metadata/signalreference.py - Gbtc
#
#  Copyright © 2026, Grid Protection Alliance.  All Rights Reserved.
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
#  02/07/2026 - J. Ritchie Carroll
#       Generated original version of source code.
#
# ******************************************************************************************************

from enum import IntEnum

class SignalKind(IntEnum):
    """
    Fundamental signal type enumeration for common EE measurements that
    represents a kind of signal, not an explicit type.

    Notes
    -----
    This enumeration represents the basic type of a signal used to suffix a
    formatted signal reference. When used in context along with an optional
    index the fundamental signal type will identify a signal's location within
    a frame of data (see `SignalReference`).

    Contrast this to the `SignalType` enumeration which further defines an
    explicit type for a signal (e.g., a voltage or current type for an angle).
    """

    ANGLE = 0
    """Phase angle."""

    MAGNITUDE = 1
    """Phase magnitude."""

    FREQUENCY = 2
    """Line frequency."""

    DFDT = 3
    """Frequency delta over time (dF/dt)."""

    STATUS = 4
    """Status flags."""

    DIGITAL = 5
    """Digital value."""

    ANALOG = 6
    """Analog value."""

    CALCULATION = 7
    """Calculated value."""

    STATISTIC = 8
    """Statistical value."""

    ALARM = 9
    """Alarm value."""

    QUALITY = 10
    """Quality flags."""

    POWCURRENT = 11
    """Point-on-wave current."""

    POWVOLTAGE = 12
    """Point-on-wave voltage."""

    UNKNOWN = -1
    """Undetermined signal type."""

    @property
    def acronym(self) -> str:
        """
        Gets the acronym for this `SignalKind`.
        """

        acronym = _SIGNALKIND_ACRONYM.get(self)
        return acronym if acronym is not None else "??"

    @staticmethod
    def parse(acronym: str) -> "SignalKind":
        """
        Gets the `SignalKind` for the specified acronym.
        """

        kind = _ACRONYM_SIGNALKIND.get(acronym.strip().upper())
        return kind if kind is not None else SignalKind.UNKNOWN


# Lookup tables for SignalKind <-> acronym conversions
_SIGNALKIND_ACRONYM: dict[SignalKind, str] = {
    SignalKind.ANGLE: "PA",         # Phase Angle
    SignalKind.MAGNITUDE: "PM",     # Phase Magnitude
    SignalKind.FREQUENCY: "FQ",     # Frequency
    SignalKind.DFDT: "DF",          # dF/dt
    SignalKind.STATUS: "SF",        # Status Flags
    SignalKind.DIGITAL: "DV",       # Digital Value
    SignalKind.ANALOG: "AV",        # Analog Value
    SignalKind.CALCULATION: "CV",   # Calculated Value
    SignalKind.STATISTIC: "ST",     # Statistical Value
    SignalKind.ALARM: "AL",         # Alarm Value
    SignalKind.QUALITY: "QF",       # Quality Flags
    SignalKind.POWCURRENT: "IW",    # Point-on-Wave Current
    SignalKind.POWVOLTAGE: "VW",    # Point-on-Wave Voltage
}

_ACRONYM_SIGNALKIND: dict[str, SignalKind] = {v: k for k, v in _SIGNALKIND_ACRONYM.items()}

class SignalReference:
    """
    Represents a signal that can be referenced by its constituent components.
    """

    __slots__ = ("acronym", "index", "kind", "cellindex")

    def __init__(self, signal: str):
        """
        Creates a new `SignalReference` by parsing a string representation.

        Parameters
        ----------
        signal : str
            String representation of this `SignalReference`.
        """

        # Signal reference may contain multiple dashes, we're interested in the last one
        split_index = signal.rfind("-")

        # Assign default values to fields
        self.index: int = 0
        """Gets or sets the signal index of this `SignalReference`."""

        self.cellindex: int = 0
        """Gets or sets the cell index, if applicable, of this `SignalReference`."""

        if split_index > -1:
            signal_type = signal[split_index + 1:].strip().upper()
            self.acronym: str = signal[:split_index].strip().upper()
            """Gets or sets the acronym of this `SignalReference`."""

            # If the length of the signal type acronym is greater than 2, then this
            # is an indexed signal type (e.g., CORDOVA-PA2)
            if len(signal_type) > 2:
                self.kind: SignalKind = SignalKind.parse(signal_type[:2])
                """Gets or sets the `SignalKind` of this `SignalReference`."""

                if self.kind != SignalKind.UNKNOWN:
                    try:
                        self.index = int(signal_type[2:])
                    except ValueError:
                        pass
            else:
                self.kind = SignalKind.parse(signal_type)
        else:
            # This represents an error - best we can do is assume entire string is the acronym
            self.acronym = signal.strip().upper()
            self.kind = SignalKind.UNKNOWN

    def __str__(self) -> str:
        return SignalReference.tostring(self.acronym, self.kind, self.index)

    def __repr__(self) -> str:
        return self.__str__()

    @staticmethod
    def tostring(acronym: str, kind: SignalKind, index: int | None = None) -> str:
        """
        Returns a string that represents the specified acronym, `SignalKind`, and optional index.

        Parameters
        ----------
        acronym : str
            Acronym portion of the desired string representation.
        kind : SignalKind
            `SignalKind` portion of the desired string representation.
        index : int | None, optional
            Index of `SignalKind` portion of the desired string representation. Defaults to None.
        """

        if index is not None and index > 0:
            return f"{acronym}-{kind.acronym}{index}"

        return f"{acronym}-{kind.acronym}"
