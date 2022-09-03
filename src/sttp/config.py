# ******************************************************************************************************
#  config.py - Gbtc
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
#  08/23/2022 - J. Ritchie Carroll
#       Generated original version of source code.
#
# ******************************************************************************************************

from .transport.constants import Defaults
import numpy as np

class Config:
    """
    Defines the STTP connection related configuration parameters.
    """

    DEFAULT_MAXRETRIES = Defaults.MAXRETRIES
    DEFAULT_RETRYINTERVAL = Defaults.RETRYINTERVAL
    DEFAULT_MAXRETRYINTERVAL = Defaults.MAXRETRYINTERVAL
    DEFAULT_AUTORECONNECT = Defaults.AUTORECONNECT
    DEFAULT_AUTOREQUESTMETADATA = Defaults.AUTOREQUESTMETADATA
    DEFAULT_AUTOSUBSCRIBE = Defaults.AUTOSUBSCRIBE
    DEFAULT_COMPRESS_PAYLOADDATA = Defaults.COMPRESS_PAYLOADDATA
    DEFAULT_COMPRESS_METADATA = Defaults.COMPRESS_METADATA
    DEFAULT_COMPRESS_SIGNALINDEXCACHE = Defaults.COMPRESS_SIGNALINDEXCACHE
    DEFAULT_METADATAFILTERS = Defaults.METADATAFILTERS
    DEFAULT_SOCKET_TIMEOUT = Defaults.SOCKET_TIMEOUT
    DEFAULT_VERSION = Defaults.VERSION

    def __init__(self,
                 maxretries: int = ...,
                 retryinterval: float = ...,
                 maxretryinterval: float = ...,
                 autoreconnect: bool = ...,
                 autorequestmetadata: bool = ...,
                 autosubscribe: bool = ...,
                 compress_payloaddata: bool = ...,
                 compress_metadata: bool = ...,
                 compress_signalindexcache: bool = ...,
                 metadatafilters: str = ...,
                 socket_timeout: float = ...,
                 version: np.byte = ...
                ):
        """
        Creates a new `Config` instance.
        """

        self.maxretries = Config.DEFAULT_MAXRETRIES if maxretries is ... else maxretries
        """
        Defines the maximum number of times to retry a connection.
	    Set value to -1 to retry infinitely.
        """

        self.retryinterval = Config.DEFAULT_RETRYINTERVAL if retryinterval is ... else retryinterval
        """
        Defines the base retry interval, in seconds. Retries will exponentially back-off
        starting from this interval.
        """

        self.maxretryinterval = Config.DEFAULT_MAXRETRYINTERVAL if maxretryinterval is ... else maxretryinterval
        """
        Defines the maximum retry interval, in seconds.
        """

        self.autoreconnect = Config.DEFAULT_AUTORECONNECT if autoreconnect is ... else autoreconnect
        """
        Defines flag that determines if connections should be automatically reattempted.
        """

        self.autorequestmetadata = Config.DEFAULT_AUTOREQUESTMETADATA if autorequestmetadata is ... else autorequestmetadata
        """
        Defines the flag that determines if metadata should be automatically requested
        upon successful connection. When True, metadata will be requested upon connection
        before subscription; otherwise, any metadata operations must be handled manually.
        """

        self.autosubscribe = Config.DEFAULT_AUTOSUBSCRIBE if autosubscribe is ... else autosubscribe
        """
        Defines the flag that determines if subscription should be handled automatically
        upon successful connection. When `autorequestmetadata` is True and
        `autosubscribe` is True, subscription will occur after reception of metadata.
        When `autorequestmetadata` is False and `autosubscribe` is True, subscription
        will occur at successful connection. When `autosubscribe` is False, any
        subscribe operations must be handled manually.
        """

        self.compress_payloaddata = Config.DEFAULT_COMPRESS_PAYLOADDATA if compress_payloaddata is ... else compress_payloaddata
        """
        Determines whether payload data is compressed.
        """

        self.compress_metadata = Config.DEFAULT_COMPRESS_METADATA if compress_metadata is ... else compress_metadata
        """
        Determines whether the metadata transfer is compressed.
        """

        self.compress_signalindexcache = Config.DEFAULT_COMPRESS_SIGNALINDEXCACHE if compress_signalindexcache is ... else compress_signalindexcache
        """
        Determines whether the signal index cache is compressed.
        """

        self.metadatafilters = Config.DEFAULT_METADATAFILTERS if metadatafilters is ... else metadatafilters
        """
        Defines any filters to be applied to incoming metadata to reduce total received metadata.
        Each filter expression should be separated by semi-colon.
        """

        self.socket_timeout = Config.DEFAULT_SOCKET_TIMEOUT if socket_timeout is ... else socket_timeout
        """
        Defines the timeout in seconds for all socket connections.
        """

        self.version = Config.DEFAULT_VERSION if version is ... else version
        """
        Defines the target STTP protocol version. This currently defaults to 2.
        """