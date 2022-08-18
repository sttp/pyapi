#******************************************************************************************************
#  datasubscriber.py - Gbtc
#
#  Copyright © 2022, Grid Protection Alliance.  All Rights Reserved.
#
#  Licensed to the Grid Protection Alliance (GPA) under one or more contributor license agreements. See
#  the NOTICE file distributed with this work for additional information regarding copyright ownership.
#  The GPA licenses this file to you under the MIT License (MIT), the "License"; you may not use this
#  file except in compliance with the License. You may obtain a copy of the License at:
#
#      http:#opensource.org/licenses/MIT
#
#  Unless agreed to in writing, the subject software distributed under the License is distributed on an
#  "AS-IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. Refer to the
#  License for the specific language governing permissions and limitations.
#
#  Code Modification History:
#  ----------------------------------------------------------------------------------------------------
#  08/17/2022 - J. Ritchie Carroll
#       Generated original version of source code.
#
#******************************************************************************************************

from gsf import Empty
from version import Version
from constants import *
from subscriptioninfo import SubscriptionInfo
from subscriberconnector import SubscriberConnector
from uuid import UUID
import numpy as np


class DataSubscriber:
    """
    Represents a client subscription for an STTP connection.
    """

    DEFAULT_COMPRESS_PAYLOADDATA = True      # Defaults to TSSC
    DEFAULT_COMPRESS_METADATA = True         # Defaults to Gzip
    DEFAULT_COMPRESS_SIGNALINDEXCACHE = True  # Defaults to Gzip
    DEFAULT_VERSION = np.byte(2)
    DEFAULT_STTP_SOURCEINFO = Version.STTP_SOURCE
    DEFAULT_STTP_VERSIONINFO = Version.STTP_VERSION
    DEFAULT_STTP_UPDATEDONINFO = Version.STTP_UPDATEDON

    def __init__(self,
                 compress_payloaddata: bool = ...,
                 compress_metadata: bool = ...,
                 compress_signalindexcache: bool = ...,
                 version: np.byte = ...,
                 sttp_sourceinfo: str = ...,
                 sttp_versioninfo: str = ...,
                 sttp_updatedoninfo: str = ...
                 ):
        """
        Creates a new `DataSubscriber`.
        """

        self._subscription = SubscriptionInfo()
        self._subscriberid = Empty.GUID
        self._encoding = OperationalEncoding.UTF8
        self._connector = SubscriberConnector()

        self.compress_payloaddata = DataSubscriber.DEFAULT_COMPRESS_PAYLOADDATA if compress_payloaddata is ... else compress_payloaddata
        """
        Determines whether payload data is compressed, defaults to TSSC.
        """

        self.compress_metadata = DataSubscriber.DEFAULT_COMPRESS_METADATA if compress_metadata is ... else compress_metadata
        """
        Determines whether the metadata transfer is compressed, defaults to GZip.
        """

        self.compress_signalindexcache = DataSubscriber.DEFAULT_COMPRESS_SIGNALINDEXCACHE if compress_signalindexcache is ... else compress_signalindexcache
        """
        Determines whether the signal index cache is compressed, defaults to GZip.
        """

        self.version = DataSubscriber.DEFAULT_VERSION if version is ... else version
        """
        Defines the STTP protocol version used by this library.
        """

        self.sttp_sourceinfo = DataSubscriber.DEFAULT_STTP_SOURCEINFO if sttp_sourceinfo is ... else sttp_sourceinfo
        """
        Defines the STTP library API title as identification information of `DataSubscriber` to a `DataPublisher`.
        """

        self.sttp_versioninfo = DataSubscriber.DEFAULT_STTP_VERSIONINFO if sttp_versioninfo is ... else sttp_versioninfo
        """
        Defines the STTP library API version as identification information of `DataSubscriber` to a `DataPublisher`.
        """

        self.sttp_updatedoninfo = DataSubscriber.DEFAULT_STTP_UPDATEDONINFO if sttp_updatedoninfo is ... else sttp_updatedoninfo
        """
        Defines when the STTP library API was last updated as identification information of `DataSubscriber` to a `DataPublisher`.
        """
