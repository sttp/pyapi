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
from gsf import Empty
from .datacolumn import DataColumn
from .datarow import DataRow
from .datatype import DataType
from typing import Callable, Dict, Iterator, List, Optional, Tuple, TYPE_CHECKING

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
        """
        Creates a new `DataTable`.
        """

        self._parent = parent
        self._name = name
        self._columnindexes: Dict[str, int] = {}
        self._columns: List[DataColumn] = []
        self._rows: List[DataRow] = []

    # Container methods for DataTable map to rows, not columns
    def __getitem__(self, key: int) -> DataRow:
        return self._rows[key]

    def __setitem__(self, key: int, value: DataRow):
        self._rows[key] = value

    def __delitem__(self, key: int):
        del self._rows[key]

    def __len__(self) -> int:
        return len(self._rows)

    def __contains__(self, item: DataRow) -> bool:
        return item in self._rows

    def __iter__(self) -> Iterator[DataRow]:
        return iter(self._rows)

    @property
    def parent(self) -> DataSet:
        """
        Gets the parent `DataSet` of the `DataTable`.
        """

        return self._parent

    @property
    def name(self) -> str:
        """
        Gets the name of the `DataTable`.
        """

        return self._name

    def clear_columns(self):
        """
        Clears the internal column collections.
        """

        self._columnindexes = {}
        self._columns = []

    def add_column(self, column: DataColumn):
        """
        Adds the specified column to the `DataTable`.
        """

        column._index = len(self._columns)
        self._columnindexes[column.name.upper()] = column.index
        self._columns.append(column)

    def column(self, columnindex: int) -> Optional[DataColumn]:
        """
        Gets the `DataColumn` at the specified column index if the index is in range;
        otherwise, None is returned.
        """

        if columnindex < 0 or columnindex > len(self._columns):
            return None

        return self._columns[columnindex]

    def column_byname(self, columnname: str) -> Optional[DataColumn]:
        """
        Gets the `DataColumn` for the specified column name if the name exists;
        otherwise, None is returned. Lookup is case-insensitive.
        """

        if (columnindex := self._columnindexes.get(columnname.upper())) is not None:
            return self.column(columnindex)

        return None

    def columnindex(self, columnname: str) -> int:
        """
        Gets the index for the specified column name if the name exists;
        otherwise, -1 is returned. Lookup is case-insensitive.
        """

        if (column := self.column_byname(columnname)) is not None:
            return column.index

        return -1

    def create_column(self, name: str, datatype: DataType, expression: str = Empty.STRING):
        """
        Creates a new `DataColumn` associated with the `DataTable`.
        Use `add_column` to add the new column to the `DataTable`.
        """

        return DataColumn(self, name, datatype, expression)

    def clone_column(self, source: DataColumn) -> DataColumn:
        """
        Creates a copy of the specified source `DataColumn` associated with the `DataTable`.
        """

        return self.create_column(source.name, source.datatype, source.expression)

    @property
    def columncount(self) -> int:
        """
        Gets the total number columns defined in the `DataTable`.
        """

        return len(self._columns)

    def clear_rows(self):
        """
        Clears the internal row collection.
        """

        self._rows = []

    def add_row(self, row: DataRow):
        """
        Adds the specified row to the `DataTable`.
        """

        self._rows.append(row)

    def row(self, rowindex: int) -> Optional[DataRow]:
        """
        Gets the `DataRow` at the specified row index if the index is in range;
        otherwise, None is returned.
        """

        if rowindex < 0 or rowindex > len(self._rows):
            return None

        return self._rows[rowindex]

    def rowswhere(self, predicate: Callable[[DataRow], bool], limit: int = -1) -> List[DataRow]:
        """
        Returns the rows matching the predicate expression. Set limit parameter
        to -1 for all matching rows.
        """

        matchingrows = []
        count = 0

        for datarow in self._rows:
            if datarow is None:
                continue

            if predicate(datarow):
                matchingrows.append(datarow)
                count += 1

                if limit > -1 and count >= limit:
                    break

        return matchingrows

    def create_row(self) -> DataRow:
        """
        Creates a new `DataRow` associated with the `DataTable`.
        Use `add_row` to add the new row to the `DataTable`.
        """

        return DataRow(self)

    def clone_row(self, source: DataRow) -> DataRow:
        """
        Creates a copy of the specified source `DataRow` associated with the `DataTable`.
        """

        row = self.create_row()

        for i in range(len(self._columns)):
            value, _ = source.value[i]
            row.value[i] = value

        return row

    @property
    def rowcount(self) -> int:
        """
        Gets the total number of rows defined in the `DataTable`.
        """

        return len(self._rows)

    def rowvalue_as_string(self, rowindex: int, columnindex: int) -> str:
        """
        Reads the row record value at the specified column index converted to a string.
        For column index out of range or any other errors, an empty string will be returned.
        """

        row = self.row(rowindex)

        return Empty.STRING if row is None else row.value_as_string(columnindex)

    def rowvalue_as_string_byname(self, rowindex: int, columnname: str) -> str:
        """
        Reads the row record value for the specified column name converted to a string.
        For column name not found or any other errors, an empty string will be returned.
        """

        row = self.row(rowindex)

        return Empty.STRING if row is None else row.value_as_string_byname(columnname)

    def __repr__(self):
        image: List[str] = [f"{self.name} ["]
        
        for i in range(self._columns):
            if i > 0:
                image.append(", ")

            image.append(str(self._columns[i]))

        image.append(f"] x {len(self._rows,)} rows")

        return "".join(image)

    def select(self, filterexpression: str, sortorder: Optional[str] = None, limit: int = -1) -> Tuple[Optional[List[DataRow]], Optional[Exception]]:
        """
        Returns the rows matching the filter expression criteria in the specified sort order.
        
        The `filterexpression` parameter should be in the syntax of a SQL WHERE expression but
        should not include the WHERE keyword. The `sortorder` parameter defines field names,
        separated by commas, that exist in the `DataTable` used to order the results. Each
        field specified in the `sortorder` can have an `ASC` or `DESC` suffix; defaults to
        `ASC` when no suffix is provided. When `sortorder` is an empty string, records will
        be returned in natural order. Set limit parameter to -1 for all matching rows. When
        `filterexpression` is an empty string, all records will be returned; any specified
        sort order and limit will still be respected.
        """

        if filterexpression is None or not filterexpression:
            filterexpression = "True"  # Return all records

        if limit > 0:
            filterexpression = f"FILTER TOP {limit} {self.name} WHERE {filterexpression}"
        else:
            filterexpression = f"FILTER {self.name} WHERE {filterexpression}"

        if sortorder is not None and sortorder:
            filterexpression += f" ORDER BY {sortorder}"

        from .filterexpressionparser import FilterExpressionParser

        expressiontree, err = FilterExpressionParser.generate_expressiontree(self, filterexpression, True)

        return (None, err) if err is not None else expressiontree.select(self)
