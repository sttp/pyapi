# ******************************************************************************************************
#  callbackerrorlistener.py - Gbtc
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
#  09/07/2022 - J. Ritchie Carroll
#       Generated original version of source code.
#
# ******************************************************************************************************

from antlr4.error.ErrorListener import ErrorListener
from typing import Callable, Optional


class CallbackErrorListener(ErrorListener):
    """
    Defines a implementation of an ANTLR error listener that reports
    any parsing exceptions to a user defined callback.
    """

    def __init__(self, callback: Optional[Callable[[str], None]] = None):
        self.parsingexception_callback = callback
        """
        Defines a callback for reporting ANTLR parsing exceptions.
        """

    def syntaxError(self, recognizer, offendingSymbol, line, column, msg, e):
        """
        Called when ANTLR parser encounters a syntax error.
        """

        if self.parsingexception_callback is not None:
            self.parsingexception_callback(msg)
