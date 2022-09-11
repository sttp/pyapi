# ******************************************************************************************************
#  dataset.py - Gbtc
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
from gsf import Convert, Empty
from .datatable import DataTable
from .datatype import DataType, parse_xsddatatype
from typing import Dict, Iterator, List, Tuple, Union, Optional
from decimal import Decimal
from datetime import datetime
from uuid import UUID
from io import BytesIO, StringIO
from xml.etree import ElementTree
from xml.etree.ElementTree import Element
import numpy as np


XMLSCHEMA_NAMESPACE = "http://www.w3.org/2001/XMLSchema"
"""
Defines schema namespace for the W3C XML Schema Definition Language (XSD) used by STTP metadata tables.
"""

EXT_XMLSCHEMADATA_NAMESPACE = "urn:schemas-microsoft-com:xml-msdata"
"""
Defines extended types for XSD elements, e.g., Guid and expression data types.
"""


def xsdformat(value: datetime) -> str:
    """
    Converts date/time value to a string in XSD XML schema format.
    """

    return value.isoformat(timespec="milliseconds")[:-1]  # 2 digit fractional second


class DataSet:
    """
    Represents an in-memory cache of records that is structured similarly to information
    defined in a database. The data set object consists of a collection of data table objects.
    See https://sttp.github.io/documentation/data-sets/ for more information.
    Note that this implementation uses a case-insensitive map for `DataTable` name lookups.
    Internally, case-insensitive lookups are accomplished using `str.upper()`.    
    """

    DEFAULT_NAME = "DataSet"

    def __init__(self,
                 name: str = ...
                 ):
        """
        Creates a new `DataSet`.
        """

        self._tables: Dict[str, DataTable] = {}

        self.name = DataSet.DEFAULT_NAME if name is ... else name
        """
        Defines the name of the `DataSet`.
        """

    # Case-insensitive get table by name; None returned when value does not exist
    def __getitem__(self, key: str) -> DataTable:
        return self.table(key)

    def __delitem__(self, key: str):
        del self._tables[key]

    def __len__(self) -> int:
        return len(self._tables)

    # Case-insensitive table name search
    def __contains__(self, item: str) -> bool:
        return self[item] is not None

    def __iter__(self) -> Iterator[DataTable]:
        return iter(self._tables.values())

    def clear_tables(self):
        """
        Clears the internal table collection.
        Any existing tables will be deleted.
        """

        self._tables = {}

    def add_table(self, table: DataTable):
        """
        Adds the specified table to the `DataSet`.
        """

        self._tables[table.name.upper()] = table

    def table(self, tablename: str) -> Optional[DataTable]:
        """
        Gets the `DataTable` for the specified table name if it exists;
        otherwise, None is returned. Lookup is case-insensitive.
        """

        return self._tables.get(tablename.upper())

    def tablenames(self) -> List[str]:
        """
        Gets the table names defined in the `DataSet`.
        """

        return [table.name for table in self._tables.values()]

    def tables(self) -> List[DataTable]:
        """
        Gets the `DataTable` instances defined in the `DataSet`.
        """

        return list(self._tables.values())

    def create_table(self, name: str) -> DataTable:
        """
        Creates a new `DataTable` associated with the `DataSet`.
        Use `add_table` to add the new table to the `DataSet`.
        """

        return DataTable(self, name)

    @property
    def tablecount(self) -> int:
        """
        Gets the total number of tables defined in the `DataSet`.
        """

        return len(self._tables)

    def remove_table(self, tablename: str) -> bool:
        """
        Removes the specified table name from the `DataSet`. Returns
        True if table was removed; otherwise, False if it did not exist.
        Lookup is case-insensitive.
        """

        return self._tables.pop(tablename.upper()) is not None

    def __repr__(self):
        image: List[str] = [f"{self.name} ["]

        for i, table in enumerate(self._tables):
            if i > 0:
                image.append(", ")

            image.append(table.name)
        image.append("]")

        return "".join(image)

    @staticmethod
    def from_xml(buffer: Union[str, bytes]) -> Tuple[DataSet, Optional[Exception]]:
        """
        Creates a new `DataSet` as read from the XML in the specified buffer.
        """

        dataset = DataSet()
        err = dataset.parse_xml(buffer)
        return dataset, err

    def parse_xml(self, buffer: Union[str, bytes]) -> Optional[Exception]:
        """
        Loads the `DataSet` from the XML in the specified buffer.
        """

        err: Optional[Exception] = None

        try:
            doc = ElementTree.fromstring(buffer)
        except Exception as ex:
            err = ex

        if err is not None:
            return err

        bufferio = StringIO(buffer) if isinstance(buffer, str) else BytesIO(buffer)
        
        namespaces: Dict[str, str] = dict(
            [node for _, node in ElementTree.iterparse(bufferio, events=["start-ns"])])

        if namespaces.get(Empty.STRING) is not None:
            del namespaces[Empty.STRING]

        return self.parse_xmldoc(doc, namespaces)

    def parse_xmldoc(self, root: Element, namespaces: Dict[str, str]) -> Optional[Exception]:
        """
        Loads the `DataSet` from an existing root XML document element.
        """

        def get_schemaprefix(target_namespace: str):
            prefix = ""

            for key in namespaces:
                if namespaces[key] == target_namespace:
                    prefix = key
                    break

            if len(prefix) > 0:
                prefix += ":"

            return prefix

        xs = get_schemaprefix(XMLSCHEMA_NAMESPACE)

        # Find schema node
        schema = root.find(f"{xs}schema", namespaces)

        if schema is None:
            return RuntimeError("failed to parse DataSet XML: Cannot find schema node")

        if (id := schema.attrib.get("id")) is None or id != root.tag:
            return RuntimeError(f"failed to parse DataSet XML: Cannot find schema node matching \"{root.tag}\"")

        # Populate DataSet schema
        self._load_schema(schema, namespaces, xs)

        # Populate DataSet records
        self._load_records(root)

        return None

    def _load_schema(self, schema: Element, namespaces: Dict[str, str], xs: str):
        EXT_PREFIX = f"{{{EXT_XMLSCHEMADATA_NAMESPACE}}}"

        # Find choice elements representing schema table definitions
        tablenodes = schema.findall(f"{xs}element/{xs}complexType/{xs}choice/{xs}element", namespaces)

        for tablenode in tablenodes:
            if (tablename := tablenode.attrib.get("name")) is None:
                continue

            datatable = self.create_table(tablename)

            # Find sequence elements representing schema table field definitions
            fieldnodes = tablenode.findall(f"{xs}complexType/{xs}sequence/{xs}element", namespaces)

            for fieldnode in fieldnodes:
                if (fieldname := fieldnode.attrib.get("name")) is None:
                    continue

                if (typename := fieldnode.attrib.get("type")) is None:
                    continue

                if typename.startswith(xs):
                    typename = typename[len(xs):]

                # Check for extended data type (allows XSD Guid field definitions)
                extdatatype = fieldnode.attrib.get(f"{EXT_PREFIX}DataType")

                datatype, found = parse_xsddatatype(typename, extdatatype)

                # Columns with unsupported XSD data types are skipped
                if not found:
                    continue

                # Check for computed expression
                expression = fieldnode.attrib.get(f"{EXT_PREFIX}Expression")

                datacolumn = datatable.create_column(fieldname, datatype, expression)

                datatable.add_column(datacolumn)

            self.add_table(datatable)

    def _load_records(self, root: Element):  # sourcery skip: low-code-quality
        # Each root node child that matches a table name represents a record
        for record in root:
            table = self.table(record.tag)

            if table is None:
                continue

            datarow = table.create_row()

            # Each child node of a record represents a field value
            for field in record:
                column = table.column_byname(field.tag)

                if column is None:
                    continue

                index = column.index
                datatype = column.datatype
                value = field.text

                if datatype == DataType.STRING:
                    datarow[index] = Empty.STRING if value is None else value
                elif datatype == DataType.GUID:
                    datarow[index] = Empty.GUID if value is None else UUID(value)
                elif datatype == DataType.DATETIME:
                    datarow[index] = Empty.DATETIME if value is None else Convert.from_str(value, datetime)
                elif datatype == DataType.BOOLEAN:
                    datarow[index] = False if value is None else bool(value)
                elif datatype == DataType.SINGLE:
                    datarow[index] = Empty.SINGLE if value is None else Convert.from_str(value, np.float32)
                elif datatype == DataType.DOUBLE:
                    datarow[index] = Empty.DOUBLE if value is None else Convert.from_str(value, np.float64)
                elif datatype == DataType.DECIMAL:
                    datarow[index] = Empty.DECIMAL if value is None else Decimal(value)
                elif datatype == DataType.INT8:
                    datarow[index] = Empty.INT8 if value is None else Convert.from_str(value, np.int8)
                elif datatype == DataType.INT16:
                    datarow[index] = Empty.INT16 if value is None else Convert.from_str(value, np.int16)
                elif datatype == DataType.INT32:
                    datarow[index] = Empty.INT32 if value is None else Convert.from_str(value, np.int32)
                elif datatype == DataType.INT64:
                    datarow[index] = Empty.INT64 if value is None else Convert.from_str(value, np.int64)
                elif datatype == DataType.UINT8:
                    datarow[index] = Empty.UINT8 if value is None else Convert.from_str(value, np.uint8)
                elif datatype == DataType.UINT16:
                    datarow[index] = Empty.UINT16 if value is None else Convert.from_str(value, np.uint16)
                elif datatype == DataType.UINT32:
                    datarow[index] = Empty.UINT32 if value is None else Convert.from_str(value, np.uint32)
                elif datatype == DataType.UINT64:
                    datarow[index] = Empty.UINT64 if value is None else Convert.from_str(value, np.uint64)
                else:
                    datarow[index] = None

            table.add_row(datarow)
