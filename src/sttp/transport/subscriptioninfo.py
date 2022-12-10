# ******************************************************************************************************
#  subscriptioninfo.py - Gbtc
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

from .constants import Defaults
import numpy as np


class SubscriptionInfo:
    """
    Defines subscription related settings for a `DataSubscriber` instance.
    """

    DEFAULT_FILTEREXPRESSION = Defaults.FILTEREXPRESSION
    DEFAULT_THROTTLED = Defaults.THROTTLED
    DEFAULT_PUBLISHINTERVAL = Defaults.PUBLISHINTERVAL
    DEFAULT_UDPDATACHANNEL = Defaults.UDPDATACHANNEL
    DEFAULT_DATACHANNEL_LOCALPORT = Defaults.DATACHANNEL_LOCALPORT
    DEFAULT_DATACHANNEL_INTERFACE = Defaults.DATACHANNEL_INTERFACE
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
                 filterexpression: str = ...,
                 throttled: bool = ...,
                 publishinterval: np.float64 = ...,
                 udpdatachannel: bool = ...,
                 datachannel_localport: np.uint16 = ...,
                 datachannel_interface: str = ...,
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
                 processinginterval: np.int32 = ...,
                 extra_connectionstring_parameters: str = ...):

        self.filterexpression = SubscriptionInfo.DEFAULT_FILTEREXPRESSION if filterexpression is ... else filterexpression
        """
        Defines the desired measurements for a subscription. Examples include:
        
        * Directly specified signal IDs (UUID values in string format):
            `38A47B0-F10B-4143-9A0A-0DBC4FFEF1E8; {E4BBFE6A-35BD-4E5B-92C9-11FF913E7877}`
        
        * Directly specified tag names:
            `DOM_GPLAINS-BUS1:VH; TVA_SHELBY-BUS1:VH`
        
        * Directly specified identifiers in "measurement key" format:
            `PPA:15; STAT:20`
        
        * A filter expression against a selection view:
            `FILTER ActiveMeasurements WHERE Company='GPA' AND SignalType='FREQ'`
        """

        self.throttled = SubscriptionInfo.DEFAULT_THROTTLED if throttled is ... else throttled
        """
        Determines if data will be published using down-sampling.
        """

        self.publishinterval = SubscriptionInfo.DEFAULT_PUBLISHINTERVAL if publishinterval is ... else publishinterval
        """
        Defines the down-sampling publish interval to use when `throttled` is `True`.
        """

        self.udpdatachannel = False if SubscriptionInfo.DEFAULT_UDPDATACHANNEL is ... else udpdatachannel
        """
        Requests that a UDP channel be used for data publication.
        """

        self.datachannel_localport = SubscriptionInfo.DEFAULT_DATACHANNEL_LOCALPORT if datachannel_localport is ... else datachannel_localport
        """
        Defines the desired UDP port to use for publication.
        """

        self.datachannel_interface = SubscriptionInfo.DEFAULT_DATACHANNEL_INTERFACE if datachannel_interface is ... else datachannel_interface
        """
        Defines the desired network interface to use for UDP publication.
        """

        self.includetime = SubscriptionInfo.DEFAULT_INCLUDETIME if includetime is ... else includetime
        """
        Determines if time should be included in non-compressed, compact measurements.
        """

        self.enabletimereasonabilitycheck = SubscriptionInfo.DEFAULT_ENABLE_TIME_REASONABILITY_CHECK if enabletimereasonabilitycheck is ... else enabletimereasonabilitycheck
        """
        Determines  if publisher should perform time reasonability checks.
        When enabled `lagtime` and `leadtime` will be used to determine if a measurement timestamp is reasonable.
        """

        self.lagtime = SubscriptionInfo.DEFAULT_LAGTIME if lagtime is ... else lagtime
        """
        Defines defines the allowed past time deviation tolerance in seconds (can be sub-second).
        Value is used to determine if a measurement timestamp is reasonable.
        Only applicable when `enabletimereasonabilitycheck` is `True`.
        """

        self.leadtime = SubscriptionInfo.DEFAULT_LEADTIME if leadtime is ... else leadtime
        """
        Defines defines the allowed future time deviation tolerance in seconds (can be sub-second).
        Value is used to determine if a measurement timestamp is reasonable.
        Only applicable when `enabletimereasonabilitycheck` is `True`.
        """

        self.uselocalclockasrealtime = SubscriptionInfo.DEFAULT_USE_LOCALCLOCK_AS_REALTIME if uselocalclockasrealtime is ... else uselocalclockasrealtime
        """
        Determines if publisher should use local clock as real time. If false,
        the timestamp of the latest measurement will be used as real-time.
        Only applicable when `enabletimereasonabilitycheck` is `True`.
        """

        self.use_millisecondresolution = SubscriptionInfo.DEFAULT_USE_MILLISECONDRESOLUTION if usemillisecondresolution is ... else usemillisecondresolution
        """
        Determines if time should be restricted to milliseconds in non-compressed, compact measurements.
        """

        self.request_nanvaluefilter = SubscriptionInfo.DEFAULT_REQUEST_NANVALUEFILTER if requestnanvaluefilter is ... else requestnanvaluefilter
        """
        Requests that the publisher filter, i.e., does not send, any NaN values.
        """

        self.starttime = SubscriptionInfo.DEFAULT_STARTTIME if starttime is ... else starttime
        """
        Defines the start time for a requested temporal data playback, i.e., a historical subscription.
        Simply by specifying a `StartTime` and `StopTime`, a subscription is considered a historical subscription.
        Note that the publisher may not support historical subscriptions, in which case the subscribe will fail.
        """

        self.stoptime = SubscriptionInfo.DEFAULT_STOPTIME if stoptime is ... else stoptime
        """
        Defines the stop time for a requested temporal data playback, i.e., a historical subscription.
        Simply by specifying a `StartTime` and `StopTime`, a subscription is considered a historical subscription.
        Note that the publisher may not support historical subscriptions, in which case the subscribe will fail.
        """

        self.constraintparameters = SubscriptionInfo.DEFAULT_CONSTRAINTPARAMETERS if constraintparameters is ... else constraintparameters
        """
        Defines any custom constraint parameters for a requested temporal data playback. This can include
        parameters that may be needed to initiate, filter, or control historical data access.
        """

        self.processinginterval = SubscriptionInfo.DEFAULT_PROCESSINGINTERVAL if processinginterval is ... else processinginterval
        """
        Defines the initial playback speed, in milliseconds, for a requested temporal data playback.
        With the exception of the values of -1 and 0, this value specifies the desired processing interval for data, i.e.,
        basically a delay, or timer interval, over which to process data. A value of -1 means to use the default processing
        interval while a value of 0 means to process data as fast as possible.
        """

        self.extra_connectionstring_parameters = SubscriptionInfo.DEFAULT_EXTRA_CONNECTIONSTRING_PARAMETERS if extra_connectionstring_parameters is ... else extra_connectionstring_parameters
        """
        Defines any extra or custom connection string parameters that may be needed for a subscription.
        """
