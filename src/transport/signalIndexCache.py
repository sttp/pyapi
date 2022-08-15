#******************************************************************************************************
#  signalIndexCache.py - Gbtc
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
#  08/15/2022 - J. Ritchie Carroll
#       Generated original version of source code.
#
#******************************************************************************************************

from ..gsf import Empty, Limits
from typing import Dict, List, Set, Tuple
from uuid import UUID
import numpy as np


class SignalIndexCache:
    """
    Represents a mapping of 32-bit runtime IDs to 128-bit globally unique Measurement IDs. The class
    additionally provides reverse lookup and an extra mapping to human-readable measurement keys.
    """

    def __init__(self):
        self.reference: Dict[np.int32, np.uint32] = dict()
        self.signalIDList: List[UUID] = []
        self.sourceList: List[str] = []
        self.idList: List[np.uint64] = []
        self.signalIDCache: Dict[UUID, np.int32] = dict()
        self.binaryLength = np.uint32(0)
        self.maxSignalIndex = np.uint32(0)
        #self.tsscDecoder = tssc.Decoder()

    def addRecord(self, signalIndex: np.int32, signalID: UUID, source: str, id: np.uint64, charSizeEstimate: np.uint32 = 1):
        index = np.uint32(len(self.signalIDList))
        self.reference[signalIndex] = index
        self.signalIDList.append(signalID)
        self.sourceList.append(source)
        self.idList.append(id)
        self.signalIDCache[signalID] = signalIndex

        if index > self.maxSignalIndex:
            self.maxSignalIndex = index

        # TODO: Uncomment when DataSubscriber is available
        # metadata = ds.LookupMetadata(signalID)

        # # Register measurement metadata if not defined already
        # if len(metadata.Source) == 0 {
        #     metadata.Source = source
        #     metadata.ID = id
        # }

        # Char size here helps provide a rough-estimate on binary length used to reserve
        # bytes for a vector, if exact size is needed call RecalculateBinaryLength first
        self.binaryLength += np.uint32(32 + len(source)*charSizeEstimate)

    def Contains(self, signalIndex: np.int32) -> bool:
        """
        Determines if the specified signalIndex exists with the SignalIndexCache.
        """
        return signalIndex in self.reference

    def SignalID(self, signalIndex: np.uint32) -> UUID:
        """
        Returns the signal ID Guid for the specified signalIndex in the SignalIndexCache.
        """
        if signalIndex in self.reference:
            return self.signalIDList[self.reference[signalIndex]]

        return Empty.GUID

    def SignalIDs(self) -> Set[UUID]:
        """
        Returns a set for all the Guid values found in the SignalIndexCache.
        """
        return set(self.signalIDList)

    def Source(self, signalIndex: np.int32) -> str:
        """
        Returns the Measurement source string for the specified signalIndex in the SignalIndexCache.
        """
        if signalIndex in self.reference:
            return self.sourceList[self.reference[signalIndex]]

        return Empty.STRING

    def ID(self, signalIndex: np.int32) -> np.uint64:
        """
        Returns the Measurement integer ID for the specified signalIndex in the SignalIndexCache.
        """
        if signalIndex in self.reference:
            return self.idList[self.reference[signalIndex]]

        return np.uint64(Limits.MAXUINT64)

    def Record(self, signalIndex: np.int32) -> Tuple[UUID, str, np.uint64, bool]:
        """
        Record returns the key Measurement values, signalID Guid, source string, and integer ID and a
        final boolean value representing find success for the specified signalIndex in the SignalIndexCache.
        """
        if signalIndex in self.reference:
            index = self.reference[signalIndex]
            return (self.signalIDList[index], self.sourceList[index], self.idList[index], True)

        return Empty.GUID, Empty.STRING, np.uint64(0), False

    def SignalIndex(self, signalID: UUID) -> np.int32:
        """
        Returns the signal index for the specified signalID Guid in the SignalIndexCache.
        """
        if signalID in self.signalIDCache:
            return self.signalIDCache[signalID]

        return -1
