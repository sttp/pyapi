# ******************************************************************************************************
#  metadata_cache.py - Gbtc
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

from record.measurement import MeasurementRecord, SignalType
from record.device import DeviceRecord
from record.phasor import PhasorRecord
from gsf import Empty
import xml.etree.ElementTree as XMLParser
from typing import Optional, List, Dict, Set, Tuple
from datetime import datetime
from uuid import UUID, uuid1
from decimal import Decimal
import numpy as np


class MetadataCache:
    """
    Represents a collection of STTP metadata.
    """

    def __init__(self, metadata_xml: str):
        # Parse metadata
        metadata = XMLParser.fromstring(metadata_xml)

        # Extract measurement records from MeasurementDetail table rows
        measurement_records: List[MeasurementRecord] = list()

        for measurement in metadata.findall("MeasurementDetail"):
            # Get element text or empty string when value is None
            def get_elementtext(elementname): return MetadataCache._get_elementtext(
                measurement, elementname)

            # Parse STTP instance name and point ID from measurement key
            (instancename, pointid) = MetadataCache._get_measurementkey(measurement)

            if pointid == 0:
                continue

            measurement_records.append(MeasurementRecord(
                # `instanceName`: Source instance name of measurement
                instancename,
                # `pointID`: STTP point ID of measurement
                pointid,
                # `signalID`: Extract signal ID, the unique measurement guid
                MetadataCache._get_guid(measurement, "SignalID"),
                # `pointTag`: Extract the measurement point tag
                get_elementtext("PointTag"),
                # `signalReference`: Extract the measurement signal reference
                get_elementtext("SignalReference"),
                # `signalTypeName`: Extract the measurement signal type name
                get_elementtext("SignalAcronym"),
                # `deviceAcronym`: Extract the measurement's parent device acronym
                get_elementtext("DeviceAcronym"),
                # `description`: Extract the measurement description name
                get_elementtext("Description"),
                # `updatedOn`: Extract the last update time for measurement metadata
                MetadataCache._get_updatedon(measurement)
            ))

        self.pointid_measurement_map: Dict[np.uint64,
                                           MeasurementRecord] = dict()

        for measurement in measurement_records:
            self.pointid_measurement_map[measurement.pointid] = measurement

        self.signalid_measurement_map: Dict[UUID, MeasurementRecord] = dict()

        for measurement in measurement_records:
            self.signalid_measurement_map[measurement.signalid] = measurement

        self.pointtag_measurement_map: Dict[str, MeasurementRecord] = dict()

        for measurement in measurement_records:
            self.pointtag_measurement_map[measurement.pointtag] = measurement

        self.signalref_measurement_map: Dict[str, MeasurementRecord] = dict()

        for measurement in measurement_records:
            self.signalref_measurement_map[measurement.signalreference] = measurement

        self.measurement_records: List[MeasurementRecord] = measurement_records

        # Extract device records from DeviceDetail table rows
        device_records: List[DeviceRecord] = list()

        for device in metadata.findall("DeviceDetail"):
            # Get element text or empty string when value is None
            def get_elementtext(elementname): return MetadataCache._get_elementtext(
                device, elementname)

            device_records.append(DeviceRecord(
                # `nodeID`: Extract node ID guid for the device
                MetadataCache._get_guid(device, "NodeID"),
                # `deviceID`: Extract device ID, the unique device guid
                MetadataCache._get_guid(device, "UniqueID"),
                # `acronym`: Alpha-numeric identifier of the device
                get_elementtext("Acronym"),
                # `name`: Free form name for the device
                get_elementtext("Name"),
                # `accessID`: Access ID for the device
                MetadataCache._get_int(device, "AccessID"),
                # `parentAcronym`: Alpha-numeric parent identifier of the device
                get_elementtext("ParentAcronym"),
                # `protocolName`: Protocol name of the device
                get_elementtext("ProtocolName"),
                # `framesPerSecond`: Data rate for the device
                MetadataCache._get_int(device, "FramesPerSecond"),
                # `companyAcronym`: Company acronym of the device
                get_elementtext("CompanyAcronym"),
                # `vendorAcronym`: Vendor acronym of the device
                get_elementtext("VendorAcronym"),
                # `vendorDeviceName`: Vendor device name of the device
                get_elementtext("VendorDeviceName"),
                # `longitude`: Longitude of the device
                MetadataCache._get_decimal(device, "Longitude"),
                # `latitude`: Latitude of the device
                MetadataCache._get_decimal(device, "Latitude"),
                # `updatedOn`: Extract the last update time for device metadata
                MetadataCache._get_updatedon(device)
            ))

        self.deviceacronym_device_map: Dict[str, DeviceRecord] = dict()

        for device in device_records:
            self.deviceacronym_device_map[device.acronym] = device

        self.deviceid_device_map: Dict[UUID, DeviceRecord] = dict()

        for device in device_records:
            self.deviceid_device_map[device.deviceid] = device

        self.device_records: List[DeviceRecord] = device_records

        # Associate measurements with parent devices
        for measurement in measurement_records:
            device = self.find_device_acronym(measurement.deviceacronym)

            if device is not None:
                measurement.device = device
                device.measurements.add(measurement)

        # Extract phasor records from PhasorDetail table rows
        phasor_records: List[PhasorRecord] = list()

        for phasor in metadata.findall("PhasorDetail"):
            # Get element text or empty string when value is None
            def get_elementtext(elementname): return MetadataCache._get_elementtext(
                phasor, elementname)

            phasor_records.append(PhasorRecord(
                # `id`: unique integer identifier for phasor
                MetadataCache._get_int(phasor, "ID"),
                # `deviceAcronym`: Alpha-numeric identifier of the associated device
                get_elementtext("DeviceAcronym"),
                # `label`: Free form label for the phasor
                get_elementtext("Label"),
                # `type`: Phasor type for the phasor
                MetadataCache._get_char(phasor, "Type"),
                # `phase`: Phasor phase for the phasor
                MetadataCache._get_char(phasor, "Phase"),
                # `sourceIndex`: Source index for the phasor
                MetadataCache._get_int(phasor, "SourceIndex"),
                # `baseKV`: BaseKV level for the phasor
                MetadataCache._get_int(phasor, "BaseKV"),
                # `updatedOn`: Extract the last update time for phasor metadata
                MetadataCache._get_updatedon(phasor)
            ))

        # Associate phasors with parent device and associated angle/magnitude measurements
        for phasor in phasor_records:
            device = self.find_device_acronym(phasor.deviceacronym)

            if device is not None:
                phasor.Device = device
                device.phasors.add(phasor)

                angle = self.find_measurement_signalreference(
                    f"{device.acronym}-PA{phasor.sourceindex}")
                magnitude = self.find_measurement_signalreference(
                    f"{device.acronym}-PM{phasor.sourceindex}")

                if angle is not None and magnitude is not None:
                    phasor.measurements.clear()

                    angle.phasor = phasor
                    phasor.measurements.append(angle)  # Must be index 0

                    magnitude.phasor = phasor
                    phasor.measurements.append(magnitude)  # Must be index 1

        self.phasorRecords: List[PhasorRecord] = phasor_records

    @staticmethod
    def _get_elementtext(elementroot, elementname: str):
        element = elementroot.find(elementname)
        return Empty.STRING if element is None else Empty.STRING if element.text is None else element.text.strip()

    @staticmethod
    def _get_measurementkey(elementroot) -> Tuple[str, np.uint64]:
        elementtext = MetadataCache._get_elementtext(elementroot, "ID")
        defaultvalue = ("_", np.uint64(0))

        try:
            parts = elementtext.split(":")

            if len(parts) != 2:
                return defaultvalue

            return (parts[0], np.uint64(parts[1]))
        except:
            return defaultvalue

    @staticmethod
    def _get_guid(elementroot, elementname: str) -> UUID:
        elementtext = MetadataCache._get_elementtext(elementroot, elementname)
        defaultvalue = uuid1()

        if elementtext == Empty.STRING:
            return defaultvalue

        try:
            return UUID(elementtext)
        except:
            return defaultvalue

    @staticmethod
    def _get_int(elementroot, elementname: str) -> int:
        elementtext = MetadataCache._get_elementtext(elementroot, elementname)
        defaultvalue = 0

        if elementtext == Empty.STRING:
            return defaultvalue

        try:
            return int(elementtext)
        except:
            return defaultvalue

    @staticmethod
    def _get_decimal(elementroot, elementname: str) -> Decimal:
        elementtext = MetadataCache._get_elementtext(elementroot, elementname)
        defaultvalue = Empty.DECIMAL

        if elementtext == Empty.STRING:
            return defaultvalue

        try:
            return Decimal(elementtext)
        except:
            return defaultvalue

    @staticmethod
    def _get_char(elementroot, elementname: str) -> str:
        elementtext = MetadataCache._get_elementtext(elementroot, elementname)
        defaultvalue = " "

        if elementtext == Empty.STRING:
            return defaultvalue

        try:
            return elementtext[0]
        except:
            return defaultvalue

    @staticmethod
    def _get_updatedon(elementroot) -> datetime:
        elementtext = MetadataCache._get_elementtext(elementroot, "UpdatedOn")
        defaultvalue = datetime.utcnow()

        if elementtext == Empty.STRING:
            return defaultvalue

        try:
            # Interestingly the Python `datetime.fromisoformat` will only
            # parse fractional seconds with 3 or 6 digits. Since in STTP
            # metadata fractional seconds often just have 2 digits, we
            # have to work much harder to make this parse properly.
            timezone = None

            if ":" in elementtext:
                tzparts = elementtext.split("-")
                count = len(tzparts)

                elementtext = "-".join(tzparts[:count - 1])
                timezone = tzparts[count - 1]

            fsparts = elementtext.split(".")

            if len(fsparts) == 1:
                return datetime.fromisoformat(elementtext)

            datetimepart = fsparts[0]
            fracsecpart = fsparts[1]

            if len(fracsecpart) == 3 or len(fracsecpart) == 6:
                return datetime.fromisoformat(elementtext)

            elementtext = f"{datetimepart}.{fracsecpart.ljust(3, '0')}"

            if timezone is not None:
                elementtext = f"{elementtext}-{timezone}"

            return datetime.fromisoformat(elementtext)
        except:
            return defaultvalue

    def find_measurement_pointid(self, pointid: np.uint64) -> Optional[MeasurementRecord]:
        if pointid in self.pointid_measurement_map:
            return self.pointid_measurement_map[pointid]

        return None

    def find_measurement_signalid(self, signalid: UUID) -> Optional[MeasurementRecord]:
        if signalid in self.signalid_measurement_map:
            return self.signalid_measurement_map[signalid]

        return None

    def find_measurement_pointtag(self, pointtag: str) -> Optional[MeasurementRecord]:
        if pointtag in self.pointtag_measurement_map:
            return self.pointtag_measurement_map[pointtag]

        return None

    def find_measurement_signalreference(self, signalreference: str) -> Optional[MeasurementRecord]:
        if signalreference in self.signalref_measurement_map:
            return self.signalref_measurement_map[signalreference]

        return None

    def find_measurements_signaltype(self, signaltype: SignalType, instancename: Optional[str] = None) -> List[MeasurementRecord]:
        return self.find_measurements_signaltypename(str(signaltype), instancename)

    def find_measurements_signaltypename(self, signaltypename: str, instancename: Optional[str] = None) -> List[MeasurementRecord]:
        matched_records: List[MeasurementRecord] = list()
        signaltypename = signaltypename.upper()

        #                             012345678901
        if signaltypename.startswith("SIGNALTYPE."):
            signaltypename = signaltypename[11:]

        for record in self.measurement_records:
            if record.signaltypename.upper() == signaltypename:
                if instancename is None or record.instancename == instancename:
                    matched_records.append(record)

        return matched_records

    def find_measurements(self, searchval: str, instancename: Optional[str] = None) -> List[MeasurementRecord]:
        records = set()

        if searchval in self.pointtag_measurement_map:
            record = self.pointtag_measurement_map[searchval]

            if instancename is None or record.instancename == instancename:
                records.add(record)

        if searchval in self.signalref_measurement_map:
            record = self.signalref_measurement_map[searchval]

            if instancename is None or record.instancename == instancename:
                records.add(record)

        for record in self.measurement_records:
            if searchval in record.description or searchval in record.deviceacronym:
                if instancename is None or record.instancename == instancename:
                    records.add(record)

        return list(records)

    @staticmethod
    def get_pointids(records: List[MeasurementRecord]) -> List[np.uint64]:
        pointids = set()

        for record in records:
            pointids.add(record.pointid)

        return list(pointids)

    def find_device_acronym(self, deviceacronym: UUID) -> Optional[DeviceRecord]:
        if deviceacronym in self.deviceacronym_device_map:
            return self.deviceacronym_device_map[deviceacronym]

        return None

    def find_device_id(self, deviceid: UUID) -> Optional[DeviceRecord]:
        if deviceid in self.deviceid_device_map:
            return self.deviceid_device_map[deviceid]

        return None

    def find_devices(self, searchval: str) -> List[DeviceRecord]:
        records = set()

        if searchval in self.deviceacronym_device_map:
            records.add(self.deviceacronym_device_map[searchval])

        for record in self.device_records:
            if (searchval in record.acronym or
                searchval in record.name or
                searchval in record.parentacronym or
                searchval in record.companyacronym or
                searchval in record.vendoracronym or
                    searchval in record.vendordevicename):
                records.add(record)

        return list(records)
