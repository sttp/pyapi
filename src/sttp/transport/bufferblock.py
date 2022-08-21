# ******************************************************************************************************
#  bufferblock.py - Gbtc
#
#  Copyright © 2022, Grid Protection Alliance.  All Rights Reserved.
#
#  Licensed to the Grid Protection Alliance (GPA) under one or more contributor license agreements. See
#  the NOTICE file distributed with this work for additional information regarding copyright ownership.
#  The GPA licenses this file to you under the MIT License (MIT), the "License"; you may not use this
#  file except in compliance with the License. You may obtain a copy of the License at =
#
#      http =//opensource.org/licenses/MIT
#
#  Unless agreed to in writing, the subject software distributed under the License is distributed on an
#  "AS-IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. Refer to the
#  License for the specific language governing permissions and limitations.
#
#  Code Modification History =
#  ----------------------------------------------------------------------------------------------------
#  08/15/2022 - J. Ritchie Carroll
#       Generated original version of source code.
#
# ******************************************************************************************************

from gsf import Empty
from uuid import UUID
from typing import Optional

class BufferBlock:
    """
    BufferBlock defines an atomic unit of data, i.e., a binary buffer, for transport in STTP.
    """

    DEFAULT_SIGNALID = Empty.GUID
    DEFAULT_BUFFER:  Optional[bytearray] = None

    def __init__(self,
                 signalid: UUID = ...,
                 buffer: bytearray = ...
                 ):

        self.signalid = BufferBlock.DEFAULT_SIGNALID if signalid is ... else signalid
        """
        Defines measurement's globally unique identifier.
        """

        self._buffer = BufferBlock.DEFAULT_BUFFER if buffer is ... else buffer

    @property
    def buffer(self) -> Optional[bytearray]:
        """
        Gets measurement buffer as an atomic unit of data, i.e., a binary buffer.
        This buffer typically represents a partial image of a larger whole.
        """
        return self._buffer
