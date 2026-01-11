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

# pyright: reportArgumentType=false
# pyright: reportReturnType=false

from __future__ import annotations
from gsf import Convert, Empty
from .datatable import DataTable
from .datatype import DataType, parse_xsddatatype
from typing import Dict, Iterator, List, Tuple
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

    def table(self, tablename: str) -> DataTable | None:
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

        for i, table in enumerate(self._tables.values()):
            if i > 0:
                image.append(", ")

            image.append(table.name)
        image.append("]")

        return "".join(image)

    @staticmethod
    def from_xml(buffer: str | bytes) -> Tuple[DataSet, Exception | None]:
        """
        Creates a new `DataSet` as read from the XML in the specified buffer.
        """

        dataset = DataSet()
        err = dataset.parse_xml(buffer)
        return dataset, err

    def parse_xml(self, buffer: str | bytes) -> Exception | None:
        """
        Loads the `DataSet` from the XML in the specified buffer.
        """

        err: Exception | None = None

        try:
            doc = ElementTree.fromstring(buffer)
        except Exception as ex:
            err = ex

        if err is not None:
            return err

        bufferio = StringIO(buffer) if isinstance(buffer, str) else BytesIO(buffer)
        
        # ElementTree.iterparse always returns str tuples for namespaces, even with BytesIO
        namespaces = {
            node[0]: node[1] 
            for _, node in ElementTree.iterparse(bufferio, events=["start-ns"])
        }

        if namespaces.get(Empty.STRING) is not None:
            del namespaces[Empty.STRING]

        return self.parse_xmldoc(doc, namespaces)

    def parse_xmldoc(self, root: Element, namespaces: Dict[str, str]) -> Exception | None:
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

    def to_xml(self, dataset_name: str = ...) -> str:
        """
        Serializes the `DataSet` to XML format.
        Returns an XML string representation of the dataset.
        """
        
        name = self.name if dataset_name is ... else dataset_name
        
        # Build XML using ElementTree
        root = Element(name)
        
        # Create schema node
        schema = ElementTree.SubElement(root, 'xs:schema')
        schema.set('id', name)
        schema.set('xmlns:xs', XMLSCHEMA_NAMESPACE)
        schema.set('xmlns:ext', EXT_XMLSCHEMADATA_NAMESPACE)
        
        # Create root element definition
        element = ElementTree.SubElement(schema, 'xs:element')
        element.set('name', name)
        
        complex_type = ElementTree.SubElement(element, 'xs:complexType')
        choice = ElementTree.SubElement(complex_type, 'xs:choice')
        choice.set('minOccurs', '0')
        choice.set('maxOccurs', 'unbounded')
        
        # Write schema definition for each table
        for table in self._tables.values():
            table_element = ElementTree.SubElement(choice, 'xs:element')
            table_element.set('name', table.name)
            
            table_complex = ElementTree.SubElement(table_element, 'xs:complexType')
            sequence = ElementTree.SubElement(table_complex, 'xs:sequence')
            
            # Write schema definition for each column
            for col_idx in range(table.columncount):
                column = table.column(col_idx)
                col_element = ElementTree.SubElement(sequence, 'xs:element')
                col_element.set('name', column.name)
                
                # Map DataType to XSD type
                xsd_type = self._datatype_to_xsd(column.datatype)
                col_element.set('type', xsd_type)
                col_element.set('minOccurs', '0')
                
                # Guid is an extended schema data type
                if column.datatype == DataType.GUID:
                    col_element.set('ext:DataType', 'System.Guid')
                
                # Computed columns define an expression
                if column.computed and column.expression:
                    col_element.set('ext:Expression', column.expression)
        
        # Write records for each table
        for table in self._tables.values():
            for row_idx in range(table.rowcount):
                row = table.row(row_idx)
                if row is None:
                    continue
                    
                record = ElementTree.SubElement(root, table.name)
                
                for col_idx in range(table.columncount):
                    column = table.column(col_idx)
                    
                    # Skip null and computed values
                    if row[col_idx] is None or column.computed:
                        continue
                    
                    field = ElementTree.SubElement(record, column.name)
                    field.text = self._value_to_xml_text(row[col_idx], column.datatype)
        
        # Generate XML with declaration
        # Use tostring with utf-8 encoding to get proper byte output, then decode to string
        xml_bytes = ElementTree.tostring(root, encoding='utf-8', xml_declaration=True)
        return xml_bytes.decode('utf-8')
    
    @staticmethod
    def _datatype_to_xsd(datatype: DataType) -> str:
        """Maps DataType enum to XSD schema type string."""
        mapping = {
            DataType.STRING: 'xs:string',
            DataType.BOOLEAN: 'xs:boolean',
            DataType.DATETIME: 'xs:dateTime',
            DataType.SINGLE: 'xs:float',
            DataType.DOUBLE: 'xs:double',
            DataType.DECIMAL: 'xs:decimal',
            DataType.GUID: 'xs:string',  # Guid uses string with ext:DataType
            DataType.INT8: 'xs:byte',
            DataType.INT16: 'xs:short',
            DataType.INT32: 'xs:int',
            DataType.INT64: 'xs:long',
            DataType.UINT8: 'xs:unsignedByte',
            DataType.UINT16: 'xs:unsignedShort',
            DataType.UINT32: 'xs:unsignedInt',
            DataType.UINT64: 'xs:unsignedLong',
        }
        return mapping.get(datatype, 'xs:string')
    
    @staticmethod
    def _value_to_xml_text(value, datatype: DataType) -> str:
        """Converts a Python value to XML text representation."""
        if datatype == DataType.BOOLEAN:
            return 'true' if value else 'false'
        elif datatype == DataType.DATETIME:
            # Format as ISO 8601 with milliseconds and Z suffix
            dt_str = value.isoformat(timespec='milliseconds')
            if '.' in dt_str:
                dt_str = dt_str.rstrip('0')
            if not dt_str.endswith('Z'):
                dt_str += 'Z'
            return dt_str
        elif datatype == DataType.GUID:
            return str(value)
        elif datatype == DataType.DECIMAL:
            return str(value)
        else:
            return str(value)

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
