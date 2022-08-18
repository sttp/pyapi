#******************************************************************************************************
#  subscriberconnector.py - Gbtc
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
#  08/17/2022 - J. Ritchie Carroll
#       Generated original version of source code.
#
#******************************************************************************************************

from gsf import Empty
from datasubscriber import DataSubscriber
from typing import Callable
from threading import Lock, Thread, Event
from readerwriterlock import rwlock
import numpy as np


class SubscriberConnector:
    """
    Represents a connector that will establish or automatically reestablish a connection from a `DataSubscriber` to a `DataPublisher`.
    """

    ERRORMESSAGE_FUNC = Callable[[str], None]
    RECONNECT_FUNC = Callable[[DataSubscriber], None]

    DEFAULT_ERRORMESSAGE_CALLBACK: ERRORMESSAGE_FUNC = lambda msg: print(msg)
    DEFAULT_RECONNECT_CALLBACK: RECONNECT_FUNC = lambda _: ...
    DEFAULT_HOSTNAME = Empty.STRING
    DEFAULT_PORT = np.uint16(0)
    DEFAULT_MAXRETRIES = np.int32(-1)
    DEFAULT_RETRYINTERVAL = np.int32(1000)
    DEFAULT_MAXRETRYINTERVAL = np.int32(30000)
    DEFAULT_AUTORECONNECT = True

    def __init__(self,
                 errormessage_callback: ERRORMESSAGE_FUNC = ...,
                 reconnect_callback: RECONNECT_FUNC = ...,
                 hostname: str = ...,
                 port: np.uint16 = ...,
                 maxretries: np.int32 = ...,
                 retryinterval: np.int32 = ...,
                 maxretryinterval: np.int32 = ...,
                 autoreconnect: bool = ...
                 ):

        self.errormessage_callback = SubscriberConnector.DEFAULT_ERRORMESSAGE_CALLBACK if errormessage_callback is ... else errormessage_callback
        """
        Called when an error message should be logged.
        """

        self.reconnect_callback = SubscriberConnector.DEFAULT_RECONNECT_CALLBACK if reconnect_callback is ... else reconnect_callback
        """
        Called when `SubscriberConnector` attempts to reconnect.
        """

        self.hostname = SubscriberConnector.DEFAULT_HOSTNAME if hostname is ... else hostname
        """
        `DataPublisher` DNS name or IP.
        """

        self.port = SubscriberConnector.DEFAULT_PORT if port is ... else port
        """
        TCP/IP listening port of the `DataPublisher`.
        """

        self.maxretries = SubscriberConnector.DEFAULT_MAXRETRIES if maxretries is ... else maxretries
        """
        MaxRetries defines the maximum number of times to retry a connection. Set value to -1 to retry infinitely.
        """

        self.retryinterval = SubscriberConnector.DEFAULT_RETRYINTERVAL if retryinterval is ... else retryinterval
        """
        defines the base retry interval, in milliseconds. Retries will exponentially back-off starting from this interval.
        """

        self.maxretryinterval = SubscriberConnector.DEFAULT_MAXRETRYINTERVAL if maxretryinterval is ... else maxretryinterval
        """
        Defines the maximum retry interval, in milliseconds.
        """

        self.autoreconnect = SubscriberConnector.DEFAULT_AUTORECONNECT if autoreconnect is ... else autoreconnect
        """
        AutoReconnect defines flag that determines if connections should be automatically reattempted.
        """

        self._connectattempt = np.int32(0)
        self._connectionrefused = False
        self._cancel = False
        #self._reconnectthread = Thread()
        self._reconnectthread_mutex = Lock()
        self._waittimer = Event()
        self._waittimer_mutex = Lock()
        self._assigninghandler_mutex = rwlock()
