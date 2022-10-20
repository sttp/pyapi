# ******************************************************************************************************
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
# ******************************************************************************************************

from __future__ import annotations
from gsf import Empty
from .constants import ConnectStatus
from typing import List, Callable, Optional, TYPE_CHECKING
from threading import Lock, Thread, Event
from concurrent.futures import ThreadPoolExecutor
from sys import stderr
import math
import numpy as np

if TYPE_CHECKING:
    from datasubscriber import DataSubscriber


class SubscriberConnector:
    """
    Represents a connector that will establish or automatically reestablish a connection from a `DataSubscriber` to a `DataPublisher`.
    """

    DEFAULT_ERRORMESSAGE_CALLBACK: Callable[[str], None] = lambda msg: print(msg, file=stderr)
    DEFAULT_RECONNECT_CALLBACK: Callable[[DataSubscriber], None] = lambda _: ...
    DEFAULT_HOSTNAME = Empty.STRING
    DEFAULT_PORT = np.uint16(0)
    DEFAULT_MAXRETRIES = np.int32(-1)
    DEFAULT_RETRYINTERVAL = 1.0
    DEFAULT_MAXRETRYINTERVAL = 30.0
    DEFAULT_AUTORECONNECT = True

    def __init__(self,
                 errormessage_callback: Callable[[str], None] = ...,
                 reconnect_callback: Callable[[DataSubscriber], None] = ...,
                 hostname: str = ...,
                 port: np.uint16 = ...,
                 maxretries: np.int32 = ...,
                 retryinterval: float = ...,
                 maxretryinterval: float = ...,
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
        Defines flag that determines if connections should be automatically reattempted.
        """

        self._connectattempt = np.int32(0)
        self._connectionrefused = False
        self._cancel = False
        self._reconnectthread: Optional[Thread] = None
        self._reconnectthread_mutex = Lock()
        self._waittimer = Event()
        self._waittimer_mutex = Lock()

        self._threadpool = ThreadPoolExecutor(thread_name_prefix="SC-PoolThread")

    def dispose(self):
        """
        Cleanly shuts down a `SubscriberConnector` that is no longer being used, e.g., during a normal application exit.
        """

        self.cancel()
        self._threadpool.shutdown(wait=False)

    def _autoreconnect(self, ds: DataSubscriber):
        if self._cancel or ds.disposing:
            return

        # Make sure to wait on any running reconnect to complete...
        self._reconnectthread_mutex.acquire()
        reconnectthread = self._reconnectthread
        self._reconnectthread_mutex.release()

        if reconnectthread is not None and reconnectthread.is_alive():
            reconnectthread.join()

        reconnectthread = Thread(target=lambda: self._run_reconnectthread(ds), name="ReconnectThread")

        self._reconnectthread_mutex.acquire()
        self._reconnectthread = reconnectthread
        self._reconnectthread_mutex.release()

        reconnectthread.start()

    def _run_reconnectthread(self, ds: DataSubscriber):
        # Reset connection attempt counter if last attempt was not refused
        if not self._connectionrefused:
            self.reset_connection()

        if self.maxretries != -1 and self._connectattempt >= self.maxretries:
            self._dispatch_errormessage("Maximum connection retries attempted. Auto-reconnect canceled.")
            return

        self._wait_for_retry()

        if self._cancel or ds.disposing:
            return

        if self._connect(ds, True) == ConnectStatus.CANCELED:
            return

        # Notify the user that reconnect attempt was completed.
        if not self._cancel and self.reconnect_callback is not None:
            self.reconnect_callback(ds)

    def _wait_for_retry(self):
        # Apply exponential back-off algorithm for retry attempt delays
        exponent = 12 if self._connectattempt > 13 else int(self._connectattempt - 1)

        retryinterval = 0.0

        if self._connectattempt > 0:
            retryinterval = self.retryinterval * math.pow(2, exponent)

        retryinterval = min(retryinterval, self.maxretryinterval)

        # Notify the user that we are attempting to reconnect.
        message: List[str] = ["Connection"]

        if self._connectattempt > 0:
            message.append(f" attempt {self._connectattempt + 1:,}")

        message.append(f" to \"{self.hostname}:{self.port}\" was terminated. ")

        if retryinterval > 0.0:
            message.append(f"Attempting to reconnect in {retryinterval:.2f} seconds...")
        else:
            message.append("Attempting to reconnect...")

        self._dispatch_errormessage("".join(message))

        waittimer = Event()

        self._waittimer_mutex.acquire()
        self._waittimer = waittimer
        self._waittimer_mutex.release()

        waittimer.wait(retryinterval)

    def connect(self, ds: DataSubscriber) -> ConnectStatus:
        """
        Initiates a connection sequence for a `DataSubscriber`
        """

        return ConnectStatus.CANCELED if self._cancel else self._connect(ds, False)

    def _connect(self, ds: DataSubscriber, autoreconnecting: bool) -> ConnectStatus:
        if self.autoreconnect:
            ds.autoreconnect_callback = lambda: self._autoreconnect(ds)

        self._cancel = False

        while not ds.disposing:
            if self.maxretries != -1 and self._connectattempt >= self.maxretries:
                self._dispatch_errormessage("Maximum connection retries attempted. Auto-reconnect canceled.")
                break

            self._connectattempt += 1

            if ds.disposing:
                return ConnectStatus.CANCELED

            if ds._connect(self.hostname, self.port, autoreconnecting) is None:
                break

            if not ds.disposing and self.retryinterval > 0:
                autoreconnecting = True
                self._wait_for_retry()

                if self._cancel:
                    return ConnectStatus.CANCELED

        if ds.disposing:
            return ConnectStatus.CANCELED

        return ConnectStatus.SUCCESS if ds.connected else ConnectStatus.FAILED

    def cancel(self):
        """
        Stops all current and future connection sequences.
        """

        if self._cancel:
            return

        self._cancel = True

        self._waittimer_mutex.acquire()
        waittimer = self._waittimer
        self._waittimer_mutex.release()

        if waittimer is not None:
            waittimer.set()

        self._reconnectthread_mutex.acquire()
        reconnectthread = self._reconnectthread
        self._reconnectthread_mutex.release()

        if reconnectthread is not None and reconnectthread.is_alive():
            reconnectthread.join()

    def reset_connection(self):
        """
        Resets `SubscriberConnector` for a new connection.
        """

        self._connectattempt = np.int32(0)
        self._cancel = False

    def _dispatch_errormessage(self, message: str):
        if self.errormessage_callback is not None:
            self._threadpool.submit(self.errormessage_callback, message)
