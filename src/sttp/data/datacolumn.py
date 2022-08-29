# ******************************************************************************************************
#  datacolumn.py - Gbtc
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
#  08/26/2022 - J. Ritchie Carroll
#       Generated original version of source code.
#
# ******************************************************************************************************

from __future__ import annotations
from gsf import Empty, normalize_enumname
from .datatype import DataType
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .datatable import DataTable


class DataColumn:
    """
    Represents a column, i.e., a field, in a `DataTable` defining a name and a data type.
    Data columns can also be computed where its value would be derived from other columns and
    functions (https://sttp.github.io/documentation/filter-expressions/) defined in an expression.
    """

    def __init__(self,
                 parent: DataTable,
                 name: str,
                 datatype: DataType,
                 expression: str = ...
                 ):
        """
        Creates a new `DataColumn`.
        """

        self._parent = parent
        self._name = name
        self._datatype = datatype
        self._expression = Empty.STRING if expression is ... or expression is None else expression
        self._computed = len(self._expression) > 0
        self._index = -1

    @property
    def parent(self) -> DataTable:
        """
        Gets the parent `DataTable` of the `DataColumn`.
        """

        return self._parent

    @property
    def name(self) -> str:
        """
        Gets the column name of the `DataColumn`.
        """

        return self._name

    @property
    def datatype(self) -> DataType:
        """
        Gets the column `DataType` enumeration value of the `DataColumn`.
        """

        return self._datatype

    @property
    def expression(self) -> str:
        """
        Gets the column expression value of the `DataColumn`, if any.
        """

        return self._expression

    @property
    def computed(self) -> bool:
        """
        Gets a flag that determines if the `DataColumn` is a computed value,
        i.e., has a defined expression.
        """

        return self._computed

    @property
    def index(self) -> int:
        """
        Gets the index of the `DataColumn` within its parent `DataTable` columns collection.
        """

        return self._index

    def __repr__(self):
        datatype = normalize_enumname(self._datatype)

        if self._computed:
            datatype = f"Computed {datatype}"

        return f"{self._name} ({datatype})"
