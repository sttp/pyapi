# ******************************************************************************************************
#  pointmetadata.py - Gbtc
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
#  08/30/2022 - J. Ritchie Carroll
#       Generated original version of source code.
#
# ******************************************************************************************************

from gsf import Limits
from typing import Callable, Tuple, Optional
import numpy as np


class CodeWords:
    ENDOFSTREAM = np.byte(0)
    POINTIDXOR4 = np.byte(1)
    POINTIDXOR8 = np.byte(2)
    POINTIDXOR12 = np.byte(3)
    POINTIDXOR16 = np.byte(4)
    POINTIDXOR20 = np.byte(5)
    POINTIDXOR24 = np.byte(6)
    POINTIDXOR32 = np.byte(7)
    TIMEDELTA1FORWARD = np.byte(8)
    TIMEDELTA2FORWARD = np.byte(9)
    TIMEDELTA3FORWARD = np.byte(10)
    TIMEDELTA4FORWARD = np.byte(11)
    TIMEDELTA1REVERSE = np.byte(12)
    TIMEDELTA2REVERSE = np.byte(13)
    TIMEDELTA3REVERSE = np.byte(14)
    TIMEDELTA4REVERSE = np.byte(15)
    TIMESTAMP2 = np.byte(16)
    TIMEXOR7BIT = np.byte(17)
    STATEFLAGS2 = np.byte(18)
    STATEFLAGS7BIT32 = np.byte(19)
    VALUE1 = np.byte(20)
    VALUE2 = np.byte(21)
    VALUE3 = np.byte(22)
    VALUEZERO = np.byte(23)
    VALUEXOR4 = np.byte(24)
    VALUEXOR8 = np.byte(25)
    VALUEXOR12 = np.byte(26)
    VALUEXOR16 = np.byte(27)
    VALUEXOR20 = np.byte(28)
    VALUEXOR24 = np.byte(29)
    VALUEXOR28 = np.byte(30)
    VALUEXOR32 = np.byte(31)


BYTE_0 = np.byte(0)
BYTE_1 = np.byte(1)
BYTE_2 = np.byte(2)

INT32_0 = np.int32(0)
INT32_1 = np.int32(1)
INT32_2 = np.int32(2)
INT32_3 = np.int32(3)
INT32_5 = np.int32(5)
INT32_6 = np.int32(6)
INT32_7 = np.int32(7)
INT32_8 = np.int32(8)

UINT32_0 = np.uint32(0)


class PointMetadata:
    def __init__(self,
                 writebits: Optional[Callable[[np.int32, np.int32], None]],
                 readbit: Optional[Callable[[], np.int32]],
                 readbits5: Optional[Callable[[], np.int32]]
                 ):
        self.prevnextpointid1 = INT32_0
        self.prevstateflags1 = UINT32_0
        self.prevstateflags2 = UINT32_0
        self.prevvalue1 = UINT32_0
        self.prevvalue2 = UINT32_0
        self.prevvalue3 = UINT32_0

        self._commandstats = np.zeros(32, np.byte)
        self._commands_sent_sincelastchange = 0

        # Bit codes for the 4 modes of encoding
        self._mode = 4

        # Mode 1 means no prefix
        self._mode21 = BYTE_0
        self._mode31 = BYTE_0
        self._mode301 = BYTE_0
        self._mode41 = CodeWords.VALUE1
        self._mode401 = CodeWords.VALUE2
        self._mode4001 = CodeWords.VALUE3
        self._startupmode = 0

        self._writebits = writebits
        self._readbit = readbit
        self._readbits5 = readbits5

    def write_code(self, code: np.int32) -> Optional[Exception]:
        if self._mode == 1:
            self._writebits(code, INT32_5)
        elif self._mode == 2:
            if code == np.int32(self._mode21):
                self._writebits(INT32_1, INT32_1)
            else:
                self._writebits(code, INT32_6)
        elif self._mode == 3:
            if code == np.int32(self._mode31):
                self._writebits(INT32_1, INT32_1)
            elif code == np.int32(self._mode301):
                self._writebits(INT32_1, INT32_2)
            else:
                self._writebits(code, INT32_7)
        elif self._mode == 4:
            if code == np.int32(self._mode41):
                self._writebits(INT32_1, INT32_1)
            elif code == np.int32(self._mode401):
                self._writebits(INT32_1, INT32_2)
            elif code == np.int32(self._mode4001):
                self._writebits(INT32_1, INT32_3)
            else:
                self._writebits(code, INT32_8)
        else:
            return RuntimeError("coding Error")

        return self._update_codestatistics(code)

    def read_code(self) -> Tuple[np.int32, Optional[Exception]]:  # sourcery skip: assign-if-exp
        if self._mode == 1:
            code = self._readbits5()
        elif self._mode == 2:
            if self._readbit() == INT32_1:
                code = np.int32(self._mode21)
            else:
                code = self._readbits5()
        elif self._mode == 3:
            if self._readbit() == INT32_1:
                code = np.int32(self._mode31)
            elif self._readbit() == INT32_1:
                code = np.int32(self._mode301)
            else:
                code = self._readbits5()
        elif self._mode == 4:
            if self._readbit() == INT32_1:
                code = np.int32(self._mode41)
            elif self._readbit() == INT32_1:
                code = np.int32(self._mode401)
            elif self._readbit() == INT32_1:
                code = np.int32(self._mode4001)
            else:
                code = self._readbits5()
        else:
            return 0, RuntimeError("unsupported compression mode")

        err = self._update_codestatistics(code)
        return code, err

    def _update_codestatistics(self, code: np.int32) -> Optional[Exception]:
        self._commands_sent_sincelastchange += 1
        self._commandstats[code] += BYTE_1

        if self._startupmode == 0 and self._commands_sent_sincelastchange > 5:
            self._startupmode += 1
            return self._adapt_commands()

        if self._startupmode == 1 and self._commands_sent_sincelastchange > 20:
            self._startupmode += 1
            return self._adapt_commands()

        if self._startupmode == 2 and self._commands_sent_sincelastchange > 100:
            return self._adapt_commands()

        return None

    def _adapt_commands(self) -> Optional[Exception]:
        code1 = BYTE_0
        count1 = 0

        code2 = BYTE_1
        count2 = 0

        code3 = BYTE_2
        count3 = 0

        total = 0

        for i in range(len(self._commandstats)):
            count = int(self._commandstats[i])
            self._commandstats[i] = BYTE_0

            total += count

            if count > count3:
                if count > count1:
                    code3 = code2
                    count3 = count2

                    code2 = code1
                    count2 = count1

                    code1 = np.byte(i)
                    count1 = count
                elif count > count2:
                    code3 = code2
                    count3 = count2

                    code2 = np.byte(i)
                    count2 = count
                else:
                    code3 = np.byte(i)
                    count3 = count

        mode1size = total * 5
        mode2size = count1 + (total - count1) * 6
        mode3size = count1 + count2 * 2 + (total - count1 - count2) * 7
        mode4size = count1 + count2 * 2 + count3 * 3 + (total - count1 - count2 - count3) * 8

        minsize = Limits.MAXINT32
        minsize = min(minsize, mode1size)
        minsize = min(minsize, mode2size)
        minsize = min(minsize, mode3size)
        minsize = min(minsize, mode4size)

        if minsize == mode1size:
            self._mode = 1
        elif minsize == mode2size:
            self._mode = 2
            self._mode21 = code1
        elif minsize == mode3size:
            self._mode = 3
            self._mode31 = code1
            self._mode301 = code2
        elif minsize == mode4size:
            self._mode = 4
            self._mode41 = code1
            self._mode401 = code2
            self._mode4001 = code3
        elif self._writebits is None:
            return RuntimeError("subscriber coding error")
        else:
            return RuntimeError("publisher coding error")

        self._commands_sent_sincelastchange = 0
        return None
