#******************************************************************************************************
#  signal_index_cache.py - Gbtc
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

from gsf import Empty, Limits
from typing import Dict, List, Set, Tuple, Optional
from uuid import UUID
import numpy as np


class SignalIndexCache:
    """
    Represents a mapping of 32-bit runtime IDs to 128-bit globally unique Measurement IDs. The class
    additionally provides reverse lookup and an extra mapping to human-readable measurement keys.
    """

    def __init__(self):
        self._reference: Dict[np.int32, np.uint32] = dict()
        self._signalidlist: List[UUID] = []
        self._sourcelist: List[str] = []
        self._idlist: List[np.uint64] = []
        self._signalidcache: Dict[UUID, np.int32] = dict()
        self._binarylength = np.uint32(0)
        self._maxsignalindex = np.uint32(0)
        #self.tsscDecoder = tssc.Decoder()

    def _addRecord(self, signalindex: np.int32, signalid: UUID, source: str, id: np.uint64, charsizeestimate: np.uint32 = 1):
        index = np.uint32(len(self._signalidlist))
        self._reference[signalindex] = index
        self._signalidlist.append(signalid)
        self._sourcelist.append(source)
        self._idlist.append(id)
        self._signalidcache[signalid] = signalindex

        if index > self._maxsignalindex:
            self._maxsignalindex = index

        # TODO: Uncomment when DataSubscriber is available
        # metadata = ds.lookup_metadata(signalID)

        # # Register measurement metadata if not defined already
        # if len(metadata.source) == 0 {
        #     metadata.source = source
        #     metadata.id = id
        # }

        # Char size here helps provide a rough-estimate on binary length used to reserve
        # bytes for a vector, if exact size is needed call RecalculateBinaryLength first
        self._binarylength += np.uint32(32 + len(source)*charsizeestimate)

    def contains(self, signalindex: np.int32) -> bool:
        """
        Determines if the specified signalindex exists with the SignalIndexCache.
        """
        return signalindex in self._reference

    def signalid(self, signalindex: np.uint32) -> UUID:
        """
        Returns the signal ID Guid for the specified signalindex in the SignalIndexCache.
        """
        if signalindex in self._reference:
            return self._signalidlist[self._reference[signalindex]]

        return Empty.GUID

    @property
    def signalids(self) -> Set[UUID]:
        """
        Gets a set for all the Guid values found in the SignalIndexCache.
        """
        return set(self._signalidlist)

    def source(self, signalindex: np.int32) -> str:
        """
        Returns the Measurement source string for the specified signalindex in the SignalIndexCache.
        """
        if signalindex in self._reference:
            return self._sourcelist[self._reference[signalindex]]

        return Empty.STRING

    def id(self, signalindex: np.int32) -> np.uint64:
        """
        Returns the Measurement integer ID for the specified signalindex in the SignalIndexCache.
        """
        if signalindex in self._reference:
            return self._idlist[self._reference[signalindex]]

        return np.uint64(Limits.MAXUINT64)

    def record(self, signalindex: np.int32) -> Tuple[UUID, str, np.uint64, bool]:
        """
        Record returns the key Measurement values, signalID Guid, source string, and integer ID and a
        final boolean value representing find success for the specified signalindex in the SignalIndexCache.
        """
        if signalindex in self._reference:
            index = self._reference[signalindex]
            return (self._signalidlist[index], self._sourcelist[index], self._idlist[index], True)

        return Empty.GUID, Empty.STRING, np.uint64(0), False

    def signalindex(self, signalid: UUID) -> np.int32:
        """
        Returns the signal index for the specified signalID Guid in the SignalIndexCache.
        """
        if signalid in self._signalidcache:
            return self._signalidcache[signalid]

        return -1

    @property
    def maxsignalindex(self) -> np.uint32:
        """
        Gets the largest signal index in the SignalIndexCache.
        """
        return self._maxsignalindex

    @property
    def count(self) -> np.uint32:
        """
        Gets the number of Measurement records that can be found in the SignalIndexCache.
        """
        return np.uint32(len(self._signalidcache))

    #def decode(ds: DataSubcriber, buffer: bytearray, subscriberID: UUID) -> Optional[Exception]