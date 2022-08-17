#******************************************************************************************************
#  measurement_metadata.py - Gbtc
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
#  08/16/2022 - J. Ritchie Carroll
#       Generated original version of source code.
#
#******************************************************************************************************

from ..gsf import Empty
from uuid import UUID
from datetime import datetime
import numpy as np


class MeasurementMetadata:
    """
    Represents the ancillary information associated with a Measurement.
    Metadata gets cached in a registry associated with a DataSubscriber.
    """

    def __init__(self,
                 signalID: UUID = ...,
                 adder: np.float64 = ...,
                 multiplier: np.float64 = ...,
                 id: np.uint64 = ...,
                 source: str = ...,
                 signaltype: str = ...,
                 signalreference: str = ...,
                 description: str = ...,
                 updatedon: datetime = ...,
                 tag: str = ...
                 ):

        self.signalid = Empty.GUID if signalID is ... else signalID
        """
        Defines measurement's globally unique identifier.
        """

        self.adder = np.float64(0) if adder is ... else adder
        """
        Defines additive value modifier. Allows for linear value adjustment. Defaults to zero.
        """

        self.multiplier = np.float64(1) if multiplier is ... else multiplier
        """
        Defines multiplicative value modifier. Allows for linear value adjustment. Defaults to one.
        """

        self.id = np.uint64(0) if id is ... else id
        """
        Defines identification number used in human-readable measurement key.
        """

        self.source = Empty.STRING if source is ... else source
        """
        Defines source used in human-readable measurement key.
        """

        self.signaltype = Empty.STRING if signaltype is ... else signaltype
        """
        Defines signal type acronym for the measurement, e.g., FREQ.
        """

        self.signalreference = Empty.STRING if signalreference is ... else signalreference
        """
        Defines reference info about a signal based on measurement original source.
        """

        self.description = Empty.STRING if description is ... else description
        """
        Defines general description for the measurement.
        """

        self.updatedon = Empty.TICKS if updatedon is ... else updatedon
        """
        Defines the timestamp of when the metadata was last updated.
        """

        self.tag = Empty.STRING if tag is ... else tag
        """
        Defines human-readable tag name or reference value used to help describe or help identify the measurement.
        """
