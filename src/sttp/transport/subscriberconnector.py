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
from subscriberconnector import SubscriberConnector
from transport.constants import ConnectStatus
from typing import Callable, Optional
from threading import Lock, Thread, Event
from concurrent.futures import ThreadPoolExecutor
from readerwriterlock.rwlock import RWLockRead
import math
import numpy as np


class SubscriberConnector:
    """
    Represents a connector that will establish or automatically reestablish a connection from a `DataSubscriber` to a `DataPublisher`.
    """

    DEFAULT_ERRORMESSAGE_CALLBACK: Callable[[str], None] = lambda msg: print(msg)
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

        self._assigninghandler_mutex = RWLockRead()
        self._assigninghandler_readmutex = self._assigninghandler_mutex.gen_rlock()
        self._assigninghandler_writemutex = self._assigninghandler_mutex.gen_wlock()

        self._threadpool = ThreadPoolExecutor()

    def autoreconnect(self, subscriber: DataSubscriber):
        if self._cancel or subscriber.disposing:
            return

        # Make sure to wait on any running reconnect to complete...
        self._reconnectthread_mutex.acquire()
        reconnectthread = self._reconnectthread
        self._reconnectthread_mutex.release()

        if reconnectthread is not None:
            reconnectthread.join()

        reconnectthread = Thread(target=lambda: self._run_reconnectthread(subscriber))

        self._reconnectthread_mutex.acquire()
        self._reconnectthread = reconnectthread
        self._reconnectthread_mutex.release()

        reconnectthread.run()

    def _run_reconnectthread(self, subscriber: DataSubscriber):
        # Reset connection attempt counter if last attempt was not refused
        if not self._connectionrefused:
            self.resetconnection()

        if self.maxretries != -1 and self._connectattempt >= self.maxretries:
            self._dispatch_errormessage("Maximum connection retries attempted. Auto-reconnect canceled.")
            return

        self.waitforretry()

        if self._cancel or subscriber.disposing:
            return

        if self._connect(subscriber, True) == ConnectStatus.CANCELED:
            return

        # Notify the user that reconnect attempt was completed.
        self.begin_callbacksync()

        if not self._cancel and self.reconnect_callback is not None:
            self.reconnect_callback(subscriber)

        self.end_callbacksync()

    def waitforretry(self):
        # Apply exponential back-off algorithm for retry attempt delays
        if self._connectattempt > 13:
            exponent = 12
        else:
            exponent = int(self._connectattempt - 1)

        retryinterval = 0.0

        if self._connectattempt > 0:
            retryinterval = self.retryinterval * math.pow(2, exponent)

        if retryinterval > self.maxretryinterval:
            retryinterval = self.maxretryinterval

        # Notify the user that we are attempting to reconnect.
        message = []

        message.append("Connection")

        if self._connectattempt > 0:
            message.append(f" attempt {self._connectAttempt + 1:,}")

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

    def connect(self, subscriber: DataSubscriber) -> ConnectStatus:
        """
        Initiates a connection sequence for a `DataSubscriber`
        """

        if self._cancel:
            return ConnectStatus.CANCELED

        return self._connect(subscriber, False)

    def _connect(self, subscriber: DataSubscriber, autoreconnecting: bool) -> ConnectStatus:
        if autoreconnecting:
            subscriber.begin_callbackassignment()
            subscriber.autoreconnect_callback = lambda: self.autoreconnect(subscriber)
            subscriber.end_callbackassignment()

        self._cancel = False

        while not subscriber.disposing:
            if self.maxretries != -1 and self._connectattempt >= self.maxretries:
                self._dispatch_errormessage("Maximum connection retries attempted. Auto-reconnect canceled.")
                break

            self.connectAttempt += 1

            if subscriber.disposing:
                return ConnectStatus.CANCELED

            if subscriber._connect(self.hostname, self.port, autoreconnecting) is None:
                break

            if not subscriber.disposing and self.retryinterval > 0:
                autoreconnecting = True
                self.waitforretry()

                if self._cancel:
                    return ConnectStatus.CANCELED

        if subscriber.disposing:
            return ConnectStatus.CANCELED

        if subscriber.connected:
            return ConnectStatus.SUCCESS

        return ConnectStatus.FAILED

    def cancel(self):
        """
        Stops all current and future connection sequences.
        """

        self._cancel.Set()

        self._waittimer_mutex.acquire()
        waittimer = self._waittimer
        self._waittimer_mutex.release()

        if waittimer is not None:
            waittimer.set()

        self._reconnectthread_mutex.acquire()
        reconnectthread = self._reconnectthread
        self._reconnectthread_mutex.release()

        if reconnectthread is not None:
            reconnectthread.join()

    def resetconnection(self):
        """
        Resets `SubscriberConnector` for a new connection.
        """

        self._connectattempt = np.int32(0)
        self._cancel = False

    def _dispatch_errormessage(self, message: str):
        self.begin_callbacksync()

        if self.errormessage_callback is not None:
            self._threadpool.submit(self.errormessage_callback, message)

        self.end_callbacksync()

    def begin_callbackassignment(self):
        """
        Informs `DataSubscriber` that a callback change has been initiated.
        """

        self._assigninghandler_writemutex.acquire()

    def begin_callbacksync(self):
        """
        Begins a callback synchronization operation.
        """

        self._assigninghandler_readmutex.acquire()

    def end_callbacksync(self):
        """
        Ends a callback synchronization operation.
        """

        self._assigninghandler_readmutex.release()

    def end_callbackassignment(self):
        """
        Informs `DataSubscriber` that a callback change has been completed.
        """

        self._assigninghandler_writemutex.release()
