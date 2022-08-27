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

from datetime import datetime
from io import StringIO
from .dataset import DataSet
from .datatable import DataTable
from typing import Dict, Iterator, List, Tuple, Union, Optional
from xml.etree import ElementTree
from xml.etree.ElementTree import Element

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
    
    return value.isoformat(sep=' ', timespec="milliseconds")[:-1] # 2 digit fractional second

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

        self._tables: Dict[str, DataTable] = dict()

        self.name = DataSet.DEFAULT_NAME if name is ... else name
        """
        Defines the name of the `DataSet`.
        """

    # Case-insensitive get table by name; None returned when value does not exist
    def __getitem__(self, key: str) -> DataTable:
        return self.table(key)

    # Case-insensitive set table by name
    def __setitem__(self, key: str, value: DataTable):
        self._tables[key.upper()] = value

    def __delitem__(self, key: str):
        del self._tables[key]

    def __len__(self) -> int:
        return len(self._tables)

    # Case-insensitive table search
    def __contains__(self, item: str) -> bool:
        return self[item] is not None

    def __iter__(self) -> Iterator[DataTable]:
        return iter(self._tables.values())

    def clear_tables(self):
        """
        Clears the internal table collection.
        Any existing tables will be deleted.
        """

        self._tables = dict()

    def add_table(self, table: DataTable):
        """
        Adds the specified table to the `DataSet`.
        """

        self._tables[table.name.upper()] = table

    def table(self, tablename: str) -> DataTable:
        """
        Gets the `DataTable` for the specified table name if it exists;
        otherwise, None is returned. Lookup is case-insensitive.
        """

        if table := self._tables.get(tablename.upper()):
            return table

        return None

    def tablenames(self) -> List[str]:
        """
        Gets the table names defined in the `DataSet`.
        """

        tablenames: List[str] = []

        for table in self._tables.values():
            tablenames.append(table.name)

        return tablenames

    def tables(self) -> List[DataTable]:
        """
        Gets the `DataTable` instances defined in the `DataSet`.
        """

        return self._tables.values()

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
        image: List[str] = []

        image.append(f"{self.name} [")
        i = 0

        for table in self._tables:
            if i > 0:
                image.append(", ")

            image.append(table.name)
            i += 1

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

        namespaces: Dict[str, str] = dict(
           [node for _, node in ElementTree.iterparse(StringIO(buffer), events=["start-ns"])])

        return self.parse_xmldoc(doc, namespaces)

    def parse_xmldoc(self, root: Element, namespaces: Dict[str, str]) -> Optional[Exception]:
        """
        Loads the `DataSet` from an existing root XML document element.
        """

        # Find schema node
        schema = root.find("schema")

        if schema is None:
            return RuntimeError("failed to parse DataSet XML: Cannot find schema node")

        id = schema.attrib["id"]

        if id is None or id != root.tag:
            return RuntimeError(f"failed to parse DataSet XML: Cannot find schema node matching \"{root.tag}\"")

        # TODO: Validate schema namespace - check against schema namespace prefix
        if not XMLSCHEMA_NAMESPACE in namespaces.values():
            return RuntimeError(f"failed to parse DataSet XML: cannot find schema namespace \"{XMLSCHEMA_NAMESPACE}\"")

        # Populate DataSet schema
        self._load_schema(schema)

        # Populate DataSet records
        self._load_records(root)

        return None

    def _load_schema(self, schema: Element):
        pass

    def _load_records(self, root: Element):
        pass