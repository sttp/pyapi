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


class codeWords:
    EndOfStream = np.byte(0)
    PointIDXor4 = np.byte(1)
    PointIDXor8 = np.byte(2)
    PointIDXor12 = np.byte(3)
    PointIDXor16 = np.byte(4)
    PointIDXor20 = np.byte(5)
    PointIDXor24 = np.byte(6)
    PointIDXor32 = np.byte(7)
    TimeDelta1Forward = np.byte(8)
    TimeDelta2Forward = np.byte(9)
    TimeDelta3Forward = np.byte(10)
    TimeDelta4Forward = np.byte(11)
    TimeDelta1Reverse = np.byte(12)
    TimeDelta2Reverse = np.byte(13)
    TimeDelta3Reverse = np.byte(14)
    TimeDelta4Reverse = np.byte(15)
    Timestamp2 = np.byte(16)
    TimeXor7Bit = np.byte(17)
    StateFlags2 = np.byte(18)
    StateFlags7Bit32 = np.byte(19)
    Value1 = np.byte(20)
    Value2 = np.byte(21)
    Value3 = np.byte(22)
    ValueZero = np.byte(23)
    ValueXor4 = np.byte(24)
    ValueXor8 = np.byte(25)
    ValueXor12 = np.byte(26)
    ValueXor16 = np.byte(27)
    ValueXor20 = np.byte(28)
    ValueXor24 = np.byte(29)
    ValueXor28 = np.byte(30)
    ValueXor32 = np.byte(31)


class PointMetadata:
    def __init__(self,
                 writeBits: Callable[[np.int32, np.int32], None],
                 readBit: Callable[[], np.int32],
                 readBits5: Callable[[], np.int32]
                 ):
        self.PrevNextPointID1 = np.int32(0)
        self.PrevStateFlags1 = np.uint32(0)
        self.PrevStateFlags2 = np.uint32(0)
        self.PrevValue1 = np.uint32(0)
        self.PrevValue2 = np.uint32(0)
        self.PrevValue3 = np.uint32(0)

        self.commandStats = np.empty(32, np.byte)
        self.commandsSentSinceLastChange = np.int32(0)

        # Bit codes for the 4 modes of encoding
        self.mode = np.byte(4)

        # Mode 1 means no prefix
        self.mode21 = np.byte(0)
        self.mode31 = np.byte(0)
        self.mode301 = np.byte(0)
        self.mode41 = codeWords.Value1
        self.mode401 = codeWords.Value2
        self.mode4001 = codeWords.Value3
        self.startupMode = np.int32(0)

        self.writeBits = writeBits
        self.readBit = readBit
        self.readBits5 = readBits5

    def WriteCode(self, code: np.int32) -> Optional[Exception]:
        if self.mode == 1:
            self.writeBits(code, 5)
        elif self.mode == 2:
            if code == np.int32(self.mode21):
                self.writeBits(1, 1)
            else:
                self.writeBits(code, 6)
        elif self.mode == 3:
            if code == np.int32(self.mode31):
                self.writeBits(1, 1)
            elif code == np.int32(self.mode301):
                self.writeBits(1, 2)
            else:
                self.writeBits(code, 7)
        elif self.mode == 4:
            if code == np.int32(self.mode41):
                self.writeBits(1, 1)
            elif code == np.int32(self.mode401):
                self.writeBits(1, 2)
            elif code == np.int32(self.mode4001):
                self.writeBits(1, 3)
            else:
                self.writeBits(code, 8)
        else:
            return RuntimeError("coding Error")

        return self.updatedCodeStatistics(code)

    def ReadCode(self) -> Tuple[np.int32, Optional[Exception]]:
        # sourcery skip: assign-if-exp
        code = np.int32(0)

        if self.mode == 1:
            code = self.readBits5()
        elif self.mode == 2:
            if self.readBit() == 1:
                code = np.int32(self.mode21)
            else:
                code = self.readBits5()
        elif self.mode == 3:
            if self.readBit() == 1:
                code = np.int32(self.mode31)
            elif self.readBit() == 1:
                code = np.int32(self.mode301)
            else:
                code = self.readBits5()
        elif self.mode == 4:
            if self.readBit() == 1:
                code = np.int32(self.mode41)
            elif self.readBit() == 1:
                code = np.int32(self.mode401)
            elif self.readBit() == 1:
                code = np.int32(self.mode4001)
            else:
                code = self.readBits5()
        else:
            return 0, RuntimeError("unsupported compression mode")

        err = self.updatedCodeStatistics(code)
        return code, err

    def updatedCodeStatistics(self, code: np.int32) -> Optional[Exception]:
        self.commandsSentSinceLastChange += 1
        self.commandStats[code] += np.byte(1)

        if self.startupMode == 0 and self.commandsSentSinceLastChange > 5:
            self.startupMode += 1
            return self.adaptCommands()

        if self.startupMode == 1 and self.commandsSentSinceLastChange > 20:
            self.startupMode += 1
            return self.adaptCommands()

        if self.startupMode == 2 and self.commandsSentSinceLastChange > 100:
            return self.adaptCommands()

        return None

    def adaptCommands(self) -> Optional[Exception]:
        code1 = np.byte(0)
        count1 = np.int32(0)

        code2 = np.byte(1)
        count2 = np.int32(0)

        code3 = np.byte(2)
        count3 = np.int32(0)

        total = np.int32(0)

        for i in range(len(self.commandStats)):
            count = np.int32(self.commandStats[i])
            self.commandStats[i] = np.byte(0)

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

        mode1Size = np.int32(total * 5)
        mode2Size = np.int32(count1 + (total - count1) * 6)
        mode3Size = np.int32(count1 + count2 * 2 + (total - count1 - count2) * 7)
        mode4Size = np.int32(count1 + count2 * 2 + count3 * 3 + (total - count1 - count2 - count3) * 8)

        minSize = np.int32(Limits.MAXINT32)

        minSize = min(minSize, mode1Size)
        minSize = min(minSize, mode2Size)
        minSize = min(minSize, mode3Size)
        minSize = min(minSize, mode4Size)

        if minSize == mode1Size:
            self.mode = 1
        elif minSize == mode2Size:
            self.mode = 2
            self.mode21 = code1
        elif minSize == mode3Size:
            self.mode = 3
            self.mode31 = code1
            self.mode301 = code2
        elif minSize == mode4Size:
            self.mode = 4
            self.mode41 = code1
            self.mode401 = code2
            self.mode4001 = code3
        elif self.writeBits is None:
            return RuntimeError("subscriber coding error")
        else:
            return RuntimeError("publisher coding error")

        self.commandsSentSinceLastChange = 0
        return None
