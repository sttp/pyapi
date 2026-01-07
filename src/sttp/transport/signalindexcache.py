# ******************************************************************************************************
#  signalindexcache.py - Gbtc
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
# ******************************************************************************************************

from __future__ import annotations
from gsf import Empty, Limits
from gsf.endianorder import BigEndian
from .tssc.decoder import Decoder
from typing import Dict, List, Set, Tuple, Optional, TYPE_CHECKING
from uuid import UUID
import numpy as np

if TYPE_CHECKING:
    from .datasubscriber import DataSubscriber


class SignalIndexCache:
    """
    Represents a mapping of 32-bit runtime IDs to 128-bit globally unique measurement IDs. The class
    additionally provides reverse lookup and an extra mapping to human-readable measurement keys.
    """

    def __init__(self):
        self._reference: Dict[np.int32, np.uint32] = {}
        self._signalidlist: List[UUID] = []
        self._sourcelist: List[str] = []
        self._idlist: List[np.uint64] = []
        self._signalidcache: Dict[UUID, np.int32] = {}
        self._binarylength = np.uint32(0)
        self._tsscdecoder: Optional[Decoder] = None

    def _add_record(self, ds: DataSubscriber, signalindex: np.int32, signalid: UUID, source: str, id: np.uint64, charsizeestimate: np.uint32 = 1):
        index = np.uint32(len(self._signalidlist))
        self._reference[signalindex] = index
        self._signalidlist.append(signalid)
        self._sourcelist.append(source)
        self._idlist.append(id)
        self._signalidcache[signalid] = signalindex

        # Lookup measurement metadata, registering it if not defined already
        metadata = ds.lookup_metadata(signalid, source, id)

        # Char size here helps provide a rough-estimate on binary length used to reserve
        # bytes for a vector, if exact size is needed call RecalculateBinaryLength first
        self._binarylength += np.uint32(32 + len(source) * charsizeestimate)

    def contains(self, signalindex: np.int32) -> bool:
        """
        Determines if the specified signalindex exists with the `SignalIndexCache`.
        """

        return signalindex in self._reference

    def signalid(self, signalindex: np.int32) -> UUID:
        """
        Returns the signal ID Guid for the specified signalindex in the `SignalIndexCache`.
        """

        if (index := self._reference.get(signalindex)) is not None:
            return self._signalidlist[index]

        return Empty.GUID

    @property
    def signalids(self) -> Set[UUID]:
        """
        Gets a set for all the Guid values found in the `SignalIndexCache`.
        """

        return set(self._signalidlist)

    def source(self, signalindex: np.int32) -> str:
        """
        Returns the `Measurement` source string for the specified signalindex in the `SignalIndexCache`.
        """

        if (index := self._reference.get(signalindex)) is not None:
            return self._sourcelist[index]

        return Empty.STRING

    def id(self, signalindex: np.int32) -> np.uint64:
        """
        Returns the `Measurement` integer ID for the specified signalindex in the `SignalIndexCache`.
        """

        if (index := self._reference.get(signalindex)) is not None:
            return self._idlist[index]

        return np.uint64(Limits.MAXUINT64)

    def record(self, signalindex: np.int32) -> Tuple[UUID, str, np.uint64, bool]:
        """
        Record returns the key `Measurement` values, signal ID Guid, source string, and integer ID and a
        final boolean value representing find success for the specified signalindex in the `SignalIndexCache`.
        """

        if (index := self._reference.get(signalindex)) is not None:
            return (self._signalidlist[index], self._sourcelist[index], self._idlist[index], True)

        return Empty.GUID, Empty.STRING, np.uint64(0), False

    def signalindex(self, signalid: UUID) -> np.int32:
        """
        Returns the signal index for the specified signal ID Guid in the `SignalIndexCache`.
        """

        if (signalindex := self._signalidcache.get(signalid)) is not None:
            return signalindex

        return -1

    @property
    def count(self) -> np.uint32:
        """
        Gets the number of `Measurement` records that can be found in the `SignalIndexCache`.
        """

        return np.uint32(len(self._signalidcache))

    def decode(self, ds: 'sttp.transport.datasubscriber.DataSubscriber', buffer: bytes) -> Tuple[UUID, Optional[Exception]]:
        """
        Parses a `SignalIndexCache` from the specified byte buffer received from a `DataPublisher`.
        """

        length = len(buffer)

        if length < 4:
            return Empty.GUID, ValueError("not enough buffer provided to parse")

        # Byte size of cache
        binarylength = BigEndian.to_uint32(buffer)
        offset = 4

        if length < binarylength:
            return Empty.GUID, ValueError("not enough buffer provided to parse")

        subscriberid = UUID(bytes=buffer[offset:offset + 16])
        offset += 16

        # Number of references
        referencecount = BigEndian.to_uint32(buffer[offset:])
        offset += 4

        for _ in range(referencecount):
            # Signal index
            signalindex = np.int32(BigEndian.to_uint32(buffer[offset:]))
            offset += 4

            # Signal ID
            signalid = UUID(bytes=buffer[offset:offset + 16])

            offset += 16

            # Measurement key Source
            sourcesize = BigEndian.to_uint32(buffer[offset:])
            offset += 4

            source = ds.decodestr(buffer[offset: offset + sourcesize])
            offset += sourcesize

            # Measurement key ID
            keyid = BigEndian.to_uint64(buffer[offset:])
            offset += 8

            self._add_record(ds, signalindex, signalid, source, keyid)

        return (subscriberid, None)

    # Publisher-side methods (for DataPublisher/SubscriberConnection)
    
    def add_measurement_key(self, signalindex: np.int32, signalid: UUID, source: str, id: np.uint64):
        """
        Adds a measurement key to the signal index cache for publisher use.
        This is a simplified version for publishers that doesn't require a DataSubscriber.
        """
        index = np.uint32(len(self._signalidlist))
        self._reference[signalindex] = index
        self._signalidlist.append(signalid)
        self._sourcelist.append(source)
        self._idlist.append(id)
        self._signalidcache[signalid] = signalindex
        
        # Rough estimate for binary length
        self._binarylength += np.uint32(32 + len(source))
    
    def get_signal_index(self, signalid: UUID) -> np.int32:
        """
        Gets the signal index for the specified signal ID.
        Returns -1 if the signal ID is not in the cache.
        """
        return self.signalindex(signalid)
    
    @property
    def signal_id_cache(self) -> Dict[np.int32, UUID]:
        """
        Gets a dictionary mapping signal indices to signal IDs.
        Used by publisher to encode signal index cache for transmission.
        """
        result = {}
        for signalindex, index in self._reference.items():
            result[signalindex] = self._signalidlist[index]
        return result
    
    def encode(self, subscriber_id: UUID) -> bytearray:
        """
        Encodes the signal index cache for transmission to a subscriber.
        Returns a bytearray containing the encoded cache.
        """
        buffer = bytearray()
        
        # Calculate binary length
        binary_length = 4 + 16 + 4  # length + subscriber_id + count
        for signalindex, index in self._reference.items():
            source = self._sourcelist[index]
            binary_length += 4 + 16 + 4 + len(source.encode('utf-8')) + 8
        
        # Write binary length
        buffer.extend(BigEndian.from_uint32(np.uint32(binary_length)))
        
        # Write subscriber ID
        buffer.extend(subscriber_id.bytes)
        
        # Write reference count
        buffer.extend(BigEndian.from_uint32(np.uint32(len(self._reference))))
        
        # Write each reference
        for signalindex, index in sorted(self._reference.items()):
            # Signal index
            buffer.extend(BigEndian.from_uint32(np.uint32(signalindex)))
            
            # Signal ID
            signalid = self._signalidlist[index]
            buffer.extend(signalid.bytes)
            
            # Source
            source = self._sourcelist[index]
            source_bytes = source.encode('utf-8')
            buffer.extend(BigEndian.from_uint32(np.uint32(len(source_bytes))))
            buffer.extend(source_bytes)
            
            # ID
            id_value = self._idlist[index]
            buffer.extend(BigEndian.from_uint64(id_value))
        
        return buffer
