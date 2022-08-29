# ******************************************************************************************************
#  metadata/record/phasor.py - Gbtc
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
# ******************************************************************************************************

from __future__ import annotations
from gsf import Empty
from typing import Optional, List, TYPE_CHECKING
from enum import IntEnum
from datetime import datetime

if TYPE_CHECKING:
    from .measurement import MeasurementRecord
    from .device import DeviceRecord

class CompositePhasorMeasurement(IntEnum):
    ANGLE = 0
    MAGNITUDE = 1


class PhasorRecord:
    """
    Represents a record of phasor metadata in the STTP.
    """

    DEFAULT_TYPE = "V"
    DEFAULT_PHASE = "+"
    DEFAULT_BASEKV = 500
    DEFAULT_UPDATEDON = Empty.DATETIME

    def __init__(self,
                 id: int,
                 deviceacronym: str,
                 label: str,
                 type: str,
                 phase: str,
                 sourceindex: int,
                 basekv: int = ...,
                 updatedon: datetime = ...
                 ):
        """
        Constructs a new `PhasorRecord`.
        """

        self._id = id
        self._deviceacronym = deviceacronym
        self._label = label
        self._type = PhasorRecord.DEFAULT_TYPE if type is None or type == Empty.STRING else type
        self._phase = PhasorRecord.DEFAULT_PHASE if phase is None or phase == Empty.STRING else phase
        self._sourceindex = sourceindex
        self._basekv = PhasorRecord.DEFAULT_BASEKV if basekv is ... or basekv == 0 else basekv
        self._updatedon = PhasorRecord.DEFAULT_UPDATEDON if updatedon is ... else updatedon

        self.device: Optional[DeviceRecord] = None
        """
        Defines the associated `DeviceRecord` for this `PhasorRecord`.
        """

        self.measurements: List[MeasurementRecord] = []
        """
        Defines the two `MeasurementRecord` values, i.e., the angle and magnitude, associated with this `PhasorRecord`.
        """

    @property
    def id(self) -> int:  # <PhasorDetail>/<ID>
        """
        Gets the unique integer identifier for this `PhasorRecord`.
        """

        return self._id

    @property
    def deviceacronym(self) -> str:  # <PhasorDetail>/<DeviceAcronym>
        """
        Gets the alpha-numeric identifier of the associated device for this `PhasorRecord`.
        """

        return self._deviceacronym

    @property
    def label(self) -> str:  # <PhasorDetail>/<Label>
        """
        Gets the free form label for this `PhasorRecord`.
        """

        return self._label

    @property
    def type(self) -> str:  # <PhasorDetail>/<Type>
        """
        Gets the phasor type, i.e., "I" or "V", for current or voltage, respectively, for this `PhasorRecord`. 
        """

        return self._type

    @property
    def phase(self) -> str:  # <PhasorDetail>/<Phase>
        """
        Gets the phase of this `PhasorRecord`, e.g., "A", "B", "C", "+", "-", "0", etc.
        """

        return self._phase

    @property
    def basekv(self) -> int:  # <PhasorDetail>/<BaseKV>
        """
        Gets the base, i.e., nominal, kV level for this `PhasorRecord`.
        """

        return self._sourceindex

    @property
    def sourceindex(self) -> int:  # <PhasorDetail>/<SourceIndex>
        """
        Gets the source index, i.e., the 1-based ordering index of the phasor in its original context, for this `PhasorRecord`.
        """

        return self._sourceindex

    @property
    def updatedon(self) -> datetime:  # <PhasorDetail>/<UpdatedOn>
        """
        Gets the `datetime` of when this `PhasorRecord` was last updated.
        """

        return self._updatedon

    @property
    def angle_measurement(self) -> Optional[MeasurementRecord]:
        """
        Gets the associated angle `MeasurementRecord`, or `None` if not available.
        """

        return None if len(self.measurements) <= CompositePhasorMeasurement.ANGLE else \
            self.measurements[CompositePhasorMeasurement.ANGLE]

    @property
    def magnitude_measurement(self) -> Optional[MeasurementRecord]:
        """
        Gets the associated magnitude `MeasurementRecord`, or `None` if not available.
        """

        return None if len(self.measurements) <= CompositePhasorMeasurement.MAGNITUDE else \
            self.measurements[CompositePhasorMeasurement.MAGNITUDE]
