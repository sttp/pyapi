# ******************************************************************************************************
#  expressiontree.py - Gbtc
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
#  09/04/2022 - J. Ritchie Carroll
#       Generated original version of source code.
#
# ******************************************************************************************************

from .datacolumn import DataColumn

class OrderByTerm:
    """
    Represents the elements parsed for a `DataColumn` specified in the "ORDER BY" keyword.
    """

    def __init__(self, column: DataColumn, ascending: bool = True, extactmatch: bool = False):
        self.column: DataColumn = column
        """
        Defines the data column referenced from an "ORDER BY" statement.
        """

        self.ascending: bool = ascending
        """
        Defines the sort direction parsed from "ORDER BY" statement.
        """

        self.extactmatch: bool = extactmatch
        """
        Defines the exact match flag parsed from "ORDER BY" statement.
        """