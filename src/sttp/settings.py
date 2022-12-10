# ******************************************************************************************************
#  settings.py - Gbtc
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


class Settings:
    """
    Defines the STTP subscription related settings.

    Notes
    -----
    The `Settings` class exists as a simplified implementation of the `SubscriptionInfo`
    class found in the `transport` namespace. Internally, the `Subscriber` class maps
    `Settings` values to a `SubscriptionInfo` instance for use with a `DataSubscriber`.
    """

    DEFAULT_THROTTLED = Defaults.THROTTLED
    DEFAULT_PUBLISHINTERVAL = Defaults.PUBLISHINTERVAL
    DEFAULT_UDPPORT = Defaults.DATACHANNEL_LOCALPORT
    DEFAULT_UDPINTERFACE = Defaults.DATACHANNEL_INTERFACE
    DEFAULT_INCLUDETIME = Defaults.INCLUDETIME
    DEFAULT_ENABLE_TIME_REASONABILITY_CHECK = Defaults.ENABLE_TIME_REASONABILITY_CHECK
    DEFAULT_LAGTIME = Defaults.LAGTIME
    DEFAULT_LEADTIME = Defaults.LEADTIME
    DEFAULT_USE_LOCALCLOCK_AS_REALTIME = Defaults.USE_LOCALCLOCK_AS_REALTIME
    DEFAULT_USE_MILLISECONDRESOLUTION = Defaults.USE_MILLISECONDRESOLUTION
    DEFAULT_REQUEST_NANVALUEFILTER = Defaults.REQUEST_NANVALUEFILTER
    DEFAULT_STARTTIME = Defaults.STARTTIME
    DEFAULT_STOPTIME = Defaults.STOPTIME
    DEFAULT_CONSTRAINTPARAMETERS = Defaults.CONSTRAINTPARAMETERS
    DEFAULT_PROCESSINGINTERVAL = Defaults.PROCESSINGINTERVAL
    DEFAULT_EXTRA_CONNECTIONSTRING_PARAMETERS = Defaults.EXTRA_CONNECTIONSTRING_PARAMETERS

    def __init__(self,
                 throttled: bool = ...,
                 publishinterval: float = ...,
                 udpport: np.uint16 = ...,
                 udpinterface: str = ...,
                 includetime: bool = ...,
                 enabletimereasonabilitycheck: bool = ...,
                 lagtime: np.float64 = ...,
                 leadtime: np.float64 = ...,
                 uselocalclockasrealtime: bool = ...,
                 usemillisecondresolution: bool = ...,
                 requestnanvaluefilter: bool = ...,
                 starttime: str = ...,
                 stoptime: str = ...,
                 constraintparameters: str = ...,
                 processinginterval: int = ...,
                 extra_connectionstring_parameters: str = ...
                 ):
        """
        Creates a new `Settings` instance.
        """

        self.throttled = Settings.DEFAULT_THROTTLED if throttled is ... else throttled
        """
        Determines if data will be published using down-sampling.
        """

        self.publishinterval = Settings.DEFAULT_PUBLISHINTERVAL if publishinterval is ... else publishinterval
        """
        Defines the down-sampling publish interval, in seconds, to use when `throttled` is True.
        """

        self.udpport = Settings.DEFAULT_UDPPORT if udpport is ... else udpport
        """
        Defines the desired UDP port to use for publication. Zero value means do not receive data on UDP,
        i.e., data will be delivered to the STTP client via TCP.
        """

        self.udpinterface = Settings.DEFAULT_UDPINTERFACE if udpinterface is ... else udpinterface
        """
        Defines the desired UDP binding interface to use for publication. Empty string means to bind
        to all interfaces.
        """

        self.includetime = Settings.DEFAULT_INCLUDETIME if includetime is ... else includetime
        """
        Determines if time should be included in non-compressed, compact measurements.
        """

        self.enabletimereasonabilitycheck = Settings.DEFAULT_ENABLE_TIME_REASONABILITY_CHECK if enabletimereasonabilitycheck is ... else enabletimereasonabilitycheck
        """
        Determines  if publisher should perform time reasonability checks.
        When enabled `lagtime` and `leadtime` will be used to determine if a measurement timestamp is reasonable.
        """

        self.lagtime = Settings.DEFAULT_LAGTIME if lagtime is ... else lagtime
        """
        Defines defines the allowed past time deviation tolerance in seconds (can be sub-second).
        Value is used to determine if a measurement timestamp is reasonable.
        Only applicable when `enabletimereasonabilitycheck` is `true`.
        """

        self.leadtime = Settings.DEFAULT_LEADTIME if leadtime is ... else leadtime
        """
        Defines defines the allowed future time deviation tolerance in seconds (can be sub-second).
        Value is used to determine if a measurement timestamp is reasonable.
        Only applicable when `enabletimereasonabilitycheck` is `true`.
        """

        self.uselocalclockasrealtime = Settings.DEFAULT_USE_LOCALCLOCK_AS_REALTIME if uselocalclockasrealtime is ... else uselocalclockasrealtime
        """
        Determines if publisher should use local clock as real time. If false,
        the timestamp of the latest measurement will be used as real-time.
        Only applicable when `enabletimereasonabilitycheck` is `true`.
        """

        self.use_millisecondresolution = Settings.DEFAULT_USE_MILLISECONDRESOLUTION if usemillisecondresolution is ... else usemillisecondresolution
        """
        Determines if time should be restricted to milliseconds in non-compressed, compact measurements.
        """

        self.request_nanvaluefilter = Settings.DEFAULT_REQUEST_NANVALUEFILTER if requestnanvaluefilter is ... else requestnanvaluefilter
        """
        Requests that the publisher filter, i.e., does not send, any NaN values.
        """

        self.starttime = Settings.DEFAULT_STARTTIME if starttime is ... else starttime
        """
        Defines the start time for a requested temporal data playback, i.e., a historical subscription.
        Simply by specifying a `starttime` and `stoptime`, a subscription is considered a historical subscription.
        Note that the publisher may not support historical subscriptions, in which case the subscribe will fail.
        """

        self.stoptime = Settings.DEFAULT_STOPTIME if stoptime is ... else stoptime
        """
        Defines the stop time for a requested temporal data playback, i.e., a historical subscription.
        Simply by specifying a `starttime` and `stoptime`, a subscription is considered a historical subscription.
        Note that the publisher may not support historical subscriptions, in which case the subscribe will fail.
        """

        self.constraintparameters = Settings.DEFAULT_CONSTRAINTPARAMETERS if constraintparameters is ... else constraintparameters
        """
        Defines any custom constraint parameters for a requested temporal data playback. This can
        include parameters that may be needed to initiate, filter, or control historical data access.
        """

        self.processinginterval = Settings.DEFAULT_PROCESSINGINTERVAL if processinginterval is ... else processinginterval
        """
        Defines the initial playback speed, in milliseconds, for a requested temporal data playback.
        With the exception of the values of -1 and 0, this value specifies the desired processing interval for data, i.e.,
        basically a delay, or timer interval, over which to process data. A value of -1 means to use the default processing
        interval while a value of 0 means to process data as fast as possible.
        """

        self.extra_connectionstring_parameters = Settings.DEFAULT_EXTRA_CONNECTIONSTRING_PARAMETERS if extra_connectionstring_parameters is ... else extra_connectionstring_parameters
        """
        Defines any extra custom connection string parameters that may be needed for a subscription.
        """
