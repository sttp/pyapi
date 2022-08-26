# ******************************************************************************************************
#  datatable.py - Gbtc
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
#  08/25/2022 - J. Ritchie Carroll
#       Generated original version of source code.
#
# ******************************************************************************************************

from __future__ import annotations
from typing import Dict, List, TYPE_CHECKING

if TYPE_CHECKING:
    from .dataset import DataSet

class DataTable:
    """
    Represents a collection of `DataColumn` objects where each data column defines a name and a data
    type. Data columns can also be computed where its value would be derived from other columns and
    functions (https://sttp.github.io/documentation/filter-expressions/) defined in an expression.
    Note that this implementation uses a case-insensitive map for `DataColumn` name lookups.
    Internally, case-insensitive lookups are accomplished using `str.upper()`.    
    """

    def __init__(self,
                 parent: DataSet,
                 name: str
                ):        
        self._parent = parent
        self._name = name
        self._columnindexes: Dict[str, int] = dict()
        self._columns: List[DataColumn] = list()
        self._rows: List[DataRow] = list()

    @property
    def parent(self) -> DataSet:
        """
        Gets the parent DataSet of the `DataTable`.
        """

        return self._parent

    @property
    def name(self) -> str:
        """
        Gets the name of the `DataTable`.
        """

        return self._name

    # def initcolumns(self):
    #     """
    #     Initializes the internal column collections.
    #     Any existing columns will be deleted.
    #     """

    #     self._columnindexes = dict()
    #     self._columns = list()

    def addcolumn(self, column: DataColumn):
        """
        Adds the specified column to the `DataTable`.
        """

        column.index = len(self._columns)
        self._columnindexes[column.name.upper()] = column.index
        self._columns.append(column)

    def column(self, columnindex: int) -> DataColumn:
        """
        Gets the `DataColumn` at the specified column index if the index is in range;
        otherwise, None is returned.
        """

        if columnindex < 0 or columnindex