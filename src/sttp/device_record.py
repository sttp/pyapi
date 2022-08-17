#******************************************************************************************************
#  device_record.py - Gbtc
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
#  02/09/2021 - J. Ritchie Carroll
#       Generated original version of source code.
#
#******************************************************************************************************

from .measurement_record import MeasurementRecord
from .phasor_record import PhasorRecord
from gsf import Empty
from typing import Set
from datetime import datetime
from uuid import UUID
from decimal import Decimal


class DeviceRecord:
    """
    Represents a record of device metadata in the STTP.
    """

    def __init__(self,
                 nodeid: UUID,
                 deviceid: UUID,
                 acronym: str,
                 name: str,
                 accessid: int,
                 parentacronym: str = Empty.STRING,
                 protocolname: str = Empty.STRING,
                 framespersecond: int = 30,
                 companyacronym: str = Empty.STRING,
                 vendoracronym: str = Empty.STRING,
                 vendordeviceName: str = Empty.STRING,
                 longitude: Decimal = Empty.DECIMAL,
                 latitude: Decimal = Empty.DECIMAL,
                 updatedon: datetime = Empty.DATETIME
                 ):
        """
        Constructs a new `DeviceRecord`.
        """
        self._nodeid = nodeid
        self._deviceid = deviceid
        self._acronym = acronym
        self._name = name
        self._accessid = accessid
        self._parentacronym = parentacronym
        self._protocolname = protocolname
        self._framespersecond = framespersecond
        self._companyacronym = companyacronym
        self._vendoracronym = vendoracronym
        self._vendordevicename = vendordeviceName
        self._longitude = longitude
        self._latitude = latitude
        self._updatedon = updatedon

        self.measurements: Set[MeasurementRecord] = set()
        """
        Gets `MeasurementRecord` values associated with this `DeviceRecord`.
        """

        self.phasors: Set[PhasorRecord] = set()
        """
        Gets `PhasorRecord` values associated with this `DeviceRecord`.
        """

    @property
    def nodeid(self) -> UUID:  # <DeviceDetail>/<NodeID>
        """
        Gets the guid-based STTP node identifier for this `DeviceRecord`.
        """
        return self._nodeid

    @property
    def deviceid(self) -> UUID:  # <DeviceDetail>/<UniqueID>
        """
        Gets the unique guid-based identifier for this `DeviceRecord`.
        """
        return self._deviceid

    @property
    def acronym(self) -> str:  # <DeviceDetail>/<Acronym>
        """
        Gets the unique alpha-numeric identifier for this `DeviceRecord`.
        """
        return self._acronym

    @property
    def name(self) -> str:  # <DeviceDetail>/<Name>
        """
        Gets the free form name of this `DeviceRecord`.
        """
        return self._name

    @property
    def accessid(self) -> int:  # <DeviceDetail>/<AccessID>
        """
        Gets the access ID (a.k.a. ID code) for this `DeviceRecord`.
        """
        return self._accessid

    @property
    def parentacronym(self) -> str:  # <DeviceDetail>/<ParentAcronym>
        """
        Gets the parent device alpha-numeric identifier for this `DeviceRecord`, if any.
        """
        return self._parentacronym

    @property
    def protocolname(self) -> str:  # <DeviceDetail>/<ProtocolName>
        """
        Gets the name of the source protocol for this `DeviceRecord`.
        """
        return self._protocolname

    @property
    def framespersecond(self) -> int:  # <DeviceDetail>/<FramesPerSecond>
        """
        Gets the data reporting rate, in data frames per second, for this `DeviceRecord`.
        """
        return self._framespersecond

    @property
    def companyacronym(self) -> str:  # <DeviceDetail>/<CompanyAcronym>
        """
        Gets the acronym of the company associated with this `DeviceRecord`.
        """
        return self._companyacronym

    @property
    def vendoracronym(self) -> str:  # <DeviceDetail>/<VendorAcronym>
        """
        Gets the acronym of the vendor associated with this `DeviceRecord`.
        """
        return self._vendoracronym

    @property
    def vendordevicename(self) -> str:  # <DeviceDetail>/<VendorDeviceName>
        """
        Gets the acronym of the vendor device name associated with this `DeviceRecord`.
        """
        return self._vendordevicename

    @property
    def longitude(self) -> Decimal:  # <DeviceDetail>/<Longitude>
        """
        Gets the longitude of this `DeviceRecord`.
        """
        return self._longitude

    @property
    def latitude(self) -> Decimal:  # <DeviceDetail>/<Latitude>
        """
        Gets the latitude of this `DeviceRecord`.
        """
        return self._latitude

    @property
    def updatedon(self) -> datetime:  # <DeviceDetail>/<UpdatedOn>
        """
        Gets the `datetime` of when this `DeviceRecord` was last updated.
        """
        return self._updatedon
