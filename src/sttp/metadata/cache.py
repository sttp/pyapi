# ******************************************************************************************************
#  metadata/cache.py - Gbtc
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

from gsf import Empty
from ..data.dataset import DataSet
from ..data.datarow import DataRow
from ..data.datatype import default_datatype
from .record.measurement import MeasurementRecord, SignalType
from .record.device import DeviceRecord
from .record.phasor import PhasorRecord
from typing import List, Dict, Tuple, Optional
from uuid import UUID, uuid1
import numpy as np


class MetadataCache:
    """
    Represents a collection of parsed STTP metadata records.
    """

    def __init__(self, dataset: DataSet = ...):

        self.signalid_measurement_map: Dict[UUID, MeasurementRecord] = {}
        """
        Defines map of unique measurement signal IDs to measurement records.
        Measurement signal IDs (a UUID) are typically unique across disparate systems.
        """

        self.id_measurement_map: Dict[np.uint64, MeasurementRecord] = {}
        """
        Defines map of measurement key IDs to measurement records.
        Measurement key IDs are typically unique for a given publisher.
        """

        self.pointtag_measurement_map: Dict[str, MeasurementRecord] = {}
        """
        Defines map of measurement point tags to measurement records.
        Measurement point tags are typically unique for a given publisher.
        """

        self.signalref_measurement_map: Dict[str, MeasurementRecord] = {}
        """
        Defines map of measurement signal references to measurement records.
        Measurement signal references are typically unique for a given publisher.
        """

        self.deviceacronym_device_map: Dict[str, DeviceRecord] = {}
        """
        Defines map of device acronym to device records.
        Device acronyms are typically unique for a given publisher.
        """

        self.deviceid_device_map: Dict[UUID, DeviceRecord] = {}
        """
        Defines map of unique device IDs to device records.
        Device IDs (a UUID) are typically unique across disparate systems.
        """

        self.measurement_records: List[MeasurementRecord] = []
        """
        Defines list of measurement records in the cache.
        """

        self.device_records: List[DeviceRecord] = []
        """
        Defines list of device records in the cache.
        """

        self.phasorRecords: List[PhasorRecord] = []
        """
        Defines list of phasor records in the cache.
        """

        if dataset is ...:
            return

        self._extract_measurements(dataset)
        self._extract_devices(dataset)
        self._extract_phasors(dataset)

    # Extract measurement records from MeasurementDetail table rows
    def _extract_measurements(self, dataset: DataSet):
        measurement_records: List[MeasurementRecord] = []

        for measurement in dataset["MeasurementDetail"]:
            get_rowvalue = lambda columnname, default = None: self._get_rowvalue(measurement, columnname, default)

            (source, id) = self._parse_measurementkey(get_rowvalue("ID", Empty.STRING))

            measurement_records.append(MeasurementRecord(
                # `signalid`: Extract signal ID, the unique measurement guid
                get_rowvalue("SignalID", uuid1()),
                # 'adder': Extract the measurement adder
                get_rowvalue("Adder", np.float64(0.0)),
                # 'multiplier': Extract the measurement multiplier
                get_rowvalue("Multiplier", np.float64(1.0)),
                # `id`: STTP numeric point ID of measurement (from measurement key)
                id,
                # `source`: Source instance name of measurement (from measurement key)
                source,
                # `signaltypename`: Extract the measurement signal type name
                get_rowvalue("SignalAcronym", "UNKN"),
                # `signalreference`: Extract the measurement signal reference
                get_rowvalue("SignalReference"),
                # `pointtag`: Extract the measurement point tag
                get_rowvalue("PointTag"),
                # `deviceacronym`: Extract the measurement's parent device acronym
                get_rowvalue("DeviceAcronym"),
                # `description`: Extract the measurement description name
                get_rowvalue("Description"),
                # `updatedon`: Extract the last update time for measurement metadata
                get_rowvalue("UpdatedOn")
            ))

        for measurement in measurement_records:
            self.id_measurement_map[measurement.id] = measurement

        for measurement in measurement_records:
            self.signalid_measurement_map[measurement.signalid] = measurement

        for measurement in measurement_records:
            self.pointtag_measurement_map[measurement.pointtag] = measurement

        for measurement in measurement_records:
            self.signalref_measurement_map[measurement.signalreference] = measurement

        self.measurement_records = measurement_records

    # Extract device records from DeviceDetail table rows
    def _extract_devices(self, dataset: DataSet):
        device_records: List[DeviceRecord] = []
        default_nodeid = uuid1()

        for device in dataset["DeviceDetail"]:
            get_rowvalue = lambda columnname, default = None: self._get_rowvalue(device, columnname, default)

            device_records.append(DeviceRecord(
                # `nodeid`: Extract node ID guid for the device
                get_rowvalue("NodeID", default_nodeid),
                # `deviceid`: Extract device ID, the unique device guid
                get_rowvalue("UniqueID", uuid1()),
                # `acronym`: Alpha-numeric identifier of the device
                get_rowvalue("Acronym"),
                # `name`: Free form name for the device
                get_rowvalue("Name"),
                # `accessid`: Access ID for the device
                get_rowvalue("AccessID"),
                # `parentacronym`: Alpha-numeric parent identifier of the device
                get_rowvalue("ParentAcronym"),
                # `protocolname`: Protocol name of the device
                get_rowvalue("ProtocolName"),
                # `framespersecond`: Data rate for the device
                get_rowvalue("FramesPerSecond", DeviceRecord.DEFAULT_FRAMESPERSECOND),
                # `companyacronym`: Company acronym of the device
                get_rowvalue("CompanyAcronym"),
                # `vendoracronym`: Vendor acronym of the device
                get_rowvalue("VendorAcronym"),
                # `vendordevicename`: Vendor device name of the device
                get_rowvalue("VendorDeviceName"),
                # `longitude`: Longitude of the device
                get_rowvalue("Longitude"),
                # `latitude`: Latitude of the device
                get_rowvalue("Latitude"),
                # `updatedon`: Extract the last update time for device metadata
                get_rowvalue("UpdatedOn")
            ))

        for device in device_records:
            self.deviceacronym_device_map[device.acronym] = device

        for device in device_records:
            self.deviceid_device_map[device.deviceid] = device

        self.device_records = device_records

        # Associate measurements with parent devices
        for measurement in self.measurement_records:
            device = self.find_device_acronym(measurement.deviceacronym)

            if device is not None:
                measurement.device = device
                device.measurements.add(measurement)

    # Extract phasor records from PhasorDetail table rows
    def _extract_phasors(self, dataset: DataSet):
        phasor_records: List[PhasorRecord] = []

        for phasor in dataset["PhasorDetail"]:
            get_rowvalue = lambda columnname, default = None: self._get_rowvalue(phasor, columnname, default)

            phasor_records.append(PhasorRecord(
                # `id`: unique integer identifier for phasor
                get_rowvalue("ID"),
                # `deviceacronym`: Alpha-numeric identifier of the associated device
                get_rowvalue("DeviceAcronym"),
                # `label`: Free form label for the phasor
                get_rowvalue("Label"),
                # `type`: Phasor type for the phasor
                get_rowvalue("Type", PhasorRecord.DEFAULT_TYPE),
                # `phase`: Phasor phase for the phasor
                get_rowvalue("Phase", PhasorRecord.DEFAULT_PHASE),
                # `sourceindex`: Source index for the phasor
                get_rowvalue("SourceIndex"),
                # `basekv`: BaseKV level for the phasor
                get_rowvalue("BaseKV"),
                # `updatedon`: Extract the last update time for phasor metadata
                get_rowvalue("UpdatedOn")
            ))

        # Associate phasors with parent device and associated angle/magnitude measurements
        for phasor in phasor_records:
            device = self.find_device_acronym(phasor.deviceacronym)

            if device is not None:
                phasor.device = device
                device.phasors.add(phasor)

                angle = self.find_measurement_signalreference(f"{device.acronym}-PA{phasor.sourceindex}")
                magnitude = self.find_measurement_signalreference(f"{device.acronym}-PM{phasor.sourceindex}")

                if angle is not None and magnitude is not None:
                    phasor.measurements.clear()

                    angle.phasor = phasor
                    phasor.measurements.append(angle)  # Must be index 0

                    magnitude.phasor = phasor
                    phasor.measurements.append(magnitude)  # Must be index 1

        self.phasorRecords = phasor_records

    def _get_rowvalue(self, row: DataRow, columnname: str, default: Optional[object] = None):
        value, err = row.value_byname(columnname)

        if value is None or err is not None:
            if default is not None:
                return default
            
            if (column := row.parent.column_byname(columnname)) is None:
                return default
            
            return default_datatype(column.datatype)
        
        return value

    def _parse_measurementkey(self, value: str) -> Tuple[str, np.uint64]:
        defaultvalue = "_", np.uint64(0)

        try:
            parts = value.split(":")
            return defaultvalue if len(parts) != 2 else (parts[0], np.uint64(parts[1]))
        except Exception:
            return defaultvalue

    def add_measurement(self, measurement: MeasurementRecord):
        self.signalid_measurement_map[measurement.signalid] = measurement

        if measurement.id > 0:
            self.id_measurement_map[measurement.id] = measurement

        if len(measurement.pointtag) > 0:
            self.pointtag_measurement_map[measurement.pointtag] = measurement

        if len(measurement.signalreference) > 0:
            self.signalref_measurement_map[measurement.signalreference] = measurement

        self.measurement_records.append(measurement)

    def find_measurement_signalid(self, signalid: UUID) -> Optional[MeasurementRecord]:
        return self.signalid_measurement_map.get(signalid)

    def find_measurement_id(self, id: np.uint64) -> Optional[MeasurementRecord]:
        return self.id_measurement_map.get(id)

    def find_measurement_pointtag(self, pointtag: str) -> Optional[MeasurementRecord]:
        return self.pointtag_measurement_map.get(pointtag)

    def find_measurement_signalreference(self, signalreference: str) -> Optional[MeasurementRecord]:
        return self.signalref_measurement_map.get(signalreference)

    def find_measurements_signaltype(self, signaltype: SignalType, instancename: Optional[str] = None) -> List[MeasurementRecord]:
        return self.find_measurements_signaltypename(signaltype.name, instancename)

    def find_measurements_signaltypename(self, signaltypename: str, instancename: Optional[str] = None) -> List[MeasurementRecord]:
        signaltypename = signaltypename.upper()

        return [ record for record in self.measurement_records if
                record.signaltypename.upper() == signaltypename and
                (instancename is None or record.instancename == instancename) ]

    def find_measurements(self, searchval: str, instancename: Optional[str] = None) -> List[MeasurementRecord]:
        records = set()

        if searchval in self.pointtag_measurement_map:
            record = self.pointtag_measurement_map[searchval]

            if instancename is None or record.instancename == instancename:
                records.add(record)

        if (record := self.signalref_measurement_map.get(searchval)) is not None:
            if instancename is None or record.instancename == instancename:
                records.add(record)

        for record in self.measurement_records:
            if (searchval in record.description or searchval in record.deviceacronym) and \
                    (instancename is None or record.instancename == instancename):
                records.add(record)

        return list(records)

    def find_device_acronym(self, deviceacronym: str) -> Optional[DeviceRecord]:
        return self.deviceacronym_device_map.get(deviceacronym)

    def find_device_id(self, deviceid: UUID) -> Optional[DeviceRecord]:
        return self.deviceid_device_map.get(deviceid)

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
