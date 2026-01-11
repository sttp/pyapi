# ******************************************************************************************************
#  test_dataset.py.py - Gbtc
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
#  08/28/2022 - J. Ritchie Carroll
#       Generated original version of source code.
#
# ******************************************************************************************************

import unittest
from src.sttp.data.dataset import DataSet
from src.sttp.data.datatable import DataTable
from src.sttp.data.datatype import DataType
from uuid import UUID, uuid1
from typing import Tuple


class TestDataSet(unittest.TestCase):

    @staticmethod
    def _create_datacolumn(datatable: DataTable, columnname: str, datatype: DataType) -> int:
        datacolumn = datatable.create_column(columnname, datatype)
        datatable.add_column(datacolumn)
        return datatable.column_byname(columnname).index # type: ignore

    @staticmethod
    def _create_dataset() -> Tuple[DataSet, int, int, UUID, UUID]:
        dataset = DataSet()
        datatable = dataset.create_table("ActiveMeasurements")

        signalid_field = TestDataSet._create_datacolumn(datatable, "SignalID", DataType.GUID)
        signaltype_field = TestDataSet._create_datacolumn(datatable, "SignalType", DataType.STRING)

        statid = uuid1()
        datarow = datatable.create_row()
        datarow.set_value(signalid_field, statid)
        datarow.set_value(signaltype_field, "STAT")
        datatable.add_row(datarow)

        freqid = uuid1()
        datarow = datatable.create_row()
        datarow.set_value(signalid_field, freqid)
        datarow.set_value(signaltype_field, "FREQ")
        datatable.add_row(datarow)

        dataset.add_table(datatable)

        return dataset, signalid_field, signaltype_field, statid, freqid

    def test_create_dataset(self):
        dataset, _, _, _, _ = TestDataSet._create_dataset()

        self.assertTrue(dataset.tablecount == 1,
                        f"test_create_dataset: expected table count of 1, received: {dataset.tablecount}")

        datatable = dataset.tables()[0]

        self.assertTrue(datatable.rowcount == 2,
                        f"test_create_dataset: expected row count of 2, received: {datatable.rowcount}")

    def test_dataset_tablenames(self):
        dataset, _, _, _, _ = TestDataSet._create_dataset()

        tablenames = dataset.tablenames()

        self.assertTrue(len(tablenames) == 1,
                        f"test_dataset_tablenames: expected table name count of 1, received: {len(tablenames)}")

        self.assertTrue(tablenames[0] == "ActiveMeasurements",
                        f"test_dataset_tablenames: expected table name of \"ActiveMeasurements\", received: \"{tablenames[0]}\"")


if __name__ == '__main__':
    unittest.main()
