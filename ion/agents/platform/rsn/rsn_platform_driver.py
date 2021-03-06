#!/usr/bin/env python

"""
@package ion.agents.platform.rsn.rsn_platform_driver
@file    ion/agents/platform/rsn/rsn_platform_driver.py
@author  Carlos Rueda
@brief   The main RSN OMS platform driver class.
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from pyon.public import log
import logging

from ion.agents.platform.platform_driver import PlatformDriver
from ion.agents.platform.util.network import InstrumentNode
from ion.agents.platform.exceptions import PlatformException
from ion.agents.platform.exceptions import PlatformDriverException
from ion.agents.platform.exceptions import PlatformConnectionException
from ion.agents.platform.rsn.oms_client_factory import CIOMSClientFactory
from ion.agents.platform.responses import NormalResponse, InvalidResponse
from ion.agents.platform.rsn.oms_event_listener import OmsEventListener

from ion.agents.platform.util import ion_ts_2_ntp


class RSNPlatformDriver(PlatformDriver):
    """
    The main RSN OMS platform driver class.
    """

    def __init__(self, pnode, evt_recv):
        """
        Creates an RSNPlatformDriver instance.

        @param pnode     Root PlatformNode defining the platform network rooted at
                         this platform.
        @param evt_recv  Listener of events generated by this driver
        """
        PlatformDriver.__init__(self, pnode, evt_recv)

        # CIOMSClient instance created by connect()
        self._rsn_oms = None

        # external event listener: we can instantiate this here as the the
        # actual http server is started via corresponding method.
        self._event_listener = OmsEventListener(self._notify_driver_event)

    def _validate_driver_configuration(self, driver_config):
        """
        Driver config must include 'oms_uri' entry.
        """
        if not 'oms_uri' in driver_config:
            log.error("'oms_uri' not present in driver_config = %s", driver_config)
            raise PlatformDriverException(msg="driver_config does not indicate 'oms_uri'")

    def configure(self, driver_config):
        """
        Nothing special done here, only calls super.configure(driver_config)

        @param driver_config with required 'oms_uri' entry.
        """
        PlatformDriver.configure(self, driver_config)

    def _assert_rsn_oms(self):
        assert self._rsn_oms is not None, "_rsn_oms object required (created via connect() call)"

    def ping(self):
        """
        Verifies communication with external platform returning "PONG" if
        this verification completes OK.

        @retval "PONG" iff all OK.
        @raise PlatformConnectionException Cannot ping external platform or
               got unexpected response.
        """
        log.debug("%r: pinging OMS...", self._platform_id)
        self._assert_rsn_oms()
        try:
            retval = self._rsn_oms.hello.ping()
        except Exception, e:
            raise PlatformConnectionException(msg="Cannot ping %s" % str(e))

        if retval is None or retval.upper() != "PONG":
            raise PlatformConnectionException(msg="Unexpected ping response: %r" % retval)

        if log.isEnabledFor(logging.DEBUG):
            log.debug("%r: ping completed: response: %s" %(self._platform_id, retval))

        return "PONG"

    def connect(self):
        """
        Creates an CIOMSClient instance, does a ping to verify connection,
        and starts event dispatch.
        """
        # create CIOMSClient:
        oms_uri = self._driver_config['oms_uri']
        log.debug("%r: creating CIOMSClient instance with oms_uri=%r",
                  self._platform_id, oms_uri)
        self._rsn_oms = CIOMSClientFactory.create_instance(oms_uri)
        log.debug("%r: CIOMSClient instance created: %s",
                  self._platform_id, self._rsn_oms)

        # ping to verify connection:
        self.ping()

        # start event dispatch:
        self._start_event_dispatch()

    def disconnect(self):
        """
        Stops event dispatch and destroys the CIOMSClient instance.
        """
        self._stop_event_dispatch()
        CIOMSClientFactory.destroy_instance(self._rsn_oms)
        self._rsn_oms = None
        log.debug("%r: CIOMSClient instance destroyed", self._platform_id)

    def get_metadata(self):
        """
        """
        self._assert_rsn_oms()
        retval = self._rsn_oms.get_platform_metadata(self._platform_id)
        log.debug("get_platform_metadata = %s", retval)

        if not self._platform_id in retval:
            raise PlatformException("Unexpected: response does not include "
                                    "requested platform '%s'" % self._platform_id)

        md = retval[self._platform_id]
        return md

    def get_attribute_values(self, attrs):
        """
        """
        log.debug("get_attribute_values: attrs=%s", attrs)

        self._assert_rsn_oms()

        # convert the ION system time from_time to NTP, as this is the time
        # format used by the RSN OMS interface:
        attrs_ntp = [(attr_id, ion_ts_2_ntp(from_time))
                     for (attr_id, from_time) in attrs]

        retval = self._rsn_oms.get_platform_attribute_values(self._platform_id,
                                                             attrs_ntp)

        if not self._platform_id in retval:
            raise PlatformException("Unexpected: response does not include "
                                    "requested platform '%s'" % self._platform_id)

        attr_values = retval[self._platform_id]

        # reported timestamps are already in NTP. Just return the dict:
        return attr_values

    def _validate_set_attribute_values(self, attrs):
        """
        Does some pre-validation of the passed values according to the
        definition of the attributes.

        NOTE: We don't check everything here, just some basics.
        TODO determine appropriate validations at this level.
        Note that the basic checks here follow what the OMS system
        will do if we just send the request directly to it. So,
        need to determine what exactly should be done on the CI side.

        @param attrs

        @return dict of errors for the offending attribute names, if any.
        """
        # TODO determine appropriate validations at this level.

        # get definitions to verify the values against
        attr_defs = self._get_platform_attributes()

        if log.isEnabledFor(logging.DEBUG):
            log.debug("validating passed attributes: %s against defs %s", attrs, attr_defs)

        # to collect errors, if any:
        error_vals = {}
        for attr_name, attr_value in attrs:

            attr_def = attr_defs.get(attr_name, None)

            if log.isEnabledFor(logging.DEBUG):
                log.debug("validating %s against %s", attr_name, str(attr_def))

            if not attr_def:
                error_vals[attr_name] = InvalidResponse.ATTRIBUTE_NAME
                log.warn("Attribute %s not in associated platform %s",
                    attr_name, self._platform_id)
                continue

            type = attr_def.get('type', None)
            units = attr_def.get('units', None)
            min_val = attr_def.get('min_val', None)
            max_val = attr_def.get('max_val', None)
            read_write = attr_def.get('read_write', None)
            group = attr_def.get('group', None)

            if "write" != read_write:
                error_vals[attr_name] = InvalidResponse.ATTRIBUTE_NOT_WRITABLE
                log.warn(
                    "Trying to set read-only attribute %s in platform %s",
                    attr_name, self._platform_id)
                continue

            #
            # TODO the following value-related checks are minimal
            #
            if type in ["float", "int"]:
                if min_val and float(attr_value) < float(min_val):
                    error_vals[attr_name] = InvalidResponse.ATTRIBUTE_VALUE_OUT_OF_RANGE
                    log.warn(
                        "Value %s for attribute %s is less than specified minimum "
                        "value %s in associated platform %s",
                        attr_value, attr_name, min_val,
                        self._platform_id)
                    continue

                if max_val and float(attr_value) > float(max_val):
                    error_vals[attr_name] = InvalidResponse.ATTRIBUTE_VALUE_OUT_OF_RANGE
                    log.warn(
                        "Value %s for attribute %s is greater than specified maximum "
                        "value %s in associated platform %s",
                        attr_value, attr_name, max_val,
                        self._platform_id)
                    continue

        return error_vals

    def set_attribute_values(self, attrs):
        """
        """
        log.debug("set_attribute_values: attrs = %s", attrs)

        self._assert_rsn_oms()

        error_vals = self._validate_set_attribute_values(attrs)
        if len(error_vals) > 0:
            # remove offending attributes for the request below
            attrs_dict = dict(attrs)
            for bad_attr_name in error_vals:
                del attrs_dict[bad_attr_name]

            # no good attributes at all?
            if len(attrs_dict) == 0:
                # just immediately return with the errors:
                return error_vals

            # else: update attrs with the good attributes:
            attrs = attrs_dict.items()

        # ok, now make the request to RSN OMS:
        retval = self._rsn_oms.set_platform_attribute_values(self._platform_id, attrs)
        log.debug("set_platform_attribute_values = %s", retval)

        if not self._platform_id in retval:
            raise PlatformException("Unexpected: response does not include "
                                    "requested platform '%s'" % self._platform_id)

        attr_values = retval[self._platform_id]

        # Note that the reported timestamps are in NTP.
        # (Timestamps indicate the time when the value was set for each attribute.)

        # ret_attr_values: dictionary to return, initialized with the error ones
        # determined above, if any:
        ret_attr_values = error_vals

        # add the info returned from RSN OMS:
        for attr_name, attr_val_ts in attr_values.iteritems():
            ret_attr_values[attr_name] = attr_val_ts

        log.debug("set_attribute_values: returning %s", ret_attr_values)

        return ret_attr_values

    def _verify_platform_id_in_response(self, response):
        """
        Verifies the presence of my platform_id in the response.

        @param response Dictionary returned by _rsn_oms

        @retval response[self._platform_id]
        """
        if not self._platform_id in response:
            msg = "unexpected: response does not contain entry for %r" % self._platform_id
            log.error(msg)
            raise PlatformException(msg=msg)

        if response[self._platform_id] == InvalidResponse.PLATFORM_ID:
            msg = "response reports invalid platform_id for %r" % self._platform_id
            log.error(msg)
            raise PlatformException(msg=msg)
        else:
            return response[self._platform_id]

    def _verify_port_id_in_response(self, port_id, dic):
        """
        Verifies the presence of port_id in the dic.

        @param port_id  The ID to verify
        @param dic Dictionary returned by _rsn_oms

        @return dic[port_id]
        """
        if not port_id in dic:
            msg = "unexpected: dic does not contain entry for %r" % port_id
            log.error(msg)
            raise PlatformException(msg=msg)

        if dic[port_id] == InvalidResponse.PORT_ID:
            msg = "%r: response reports invalid port_id for %r" % (
                                 self._platform_id, port_id)
            log.error(msg)
            raise PlatformException(msg=msg)
        else:
            return dic[port_id]

    def _verify_instrument_id_in_response(self, port_id, instrument_id, dic):
        """
        Verifies the presence of instrument_id in the dic.

        @param port_id        Used for error reporting
        @param instrument_id  The ID to verify
        @param dic            Dictionary returned by _rsn_oms

        @return dic[instrument_id]
        """
        if not instrument_id in dic:
            msg = "unexpected: dic does not contain entry for %r" % instrument_id
            log.error(msg)
            raise PlatformException(msg=msg)

        return dic[instrument_id]

    def connect_instrument(self, port_id, instrument_id, attributes):
        log.debug("%r: connect_instrument: port_id=%r instrument_id=%r attributes=%s",
                  self._platform_id, port_id, instrument_id, attributes)

        self._assert_rsn_oms()

        response = self._rsn_oms.connect_instrument(self._platform_id, port_id, instrument_id, attributes)
        log.debug("%r: connect_instrument response: %s",
            self._platform_id, response)

        dic_plat = self._verify_platform_id_in_response(response)
        port_dic = self._verify_port_id_in_response(port_id, dic_plat)
        instr_res = self._verify_instrument_id_in_response(port_id, instrument_id, port_dic)

        # update local image if instrument was actually connected in this call:
        if isinstance(instr_res, dict):
            attrs = instr_res
            instrumentNode = InstrumentNode(instrument_id, attrs)
            self._pnode.ports[port_id].add_instrument(instrumentNode)
            log.debug("%r: port_id=%s connect_instrument: local image updated: %s",
                      self._platform_id, port_id, instrument_id)

        return dic_plat  # note: return the dic for the platform

    def disconnect_instrument(self, port_id, instrument_id):
        log.debug("%r: disconnect_instrument: port_id=%r instrument_id=%r",
                  self._platform_id, port_id, instrument_id)

        self._assert_rsn_oms()

        response = self._rsn_oms.disconnect_instrument(self._platform_id, port_id, instrument_id)
        log.debug("%r: disconnect_instrument response: %s",
                  self._platform_id, response)

        dic_plat = self._verify_platform_id_in_response(response)
        port_dic = self._verify_port_id_in_response(port_id, dic_plat)
        instr_res = self._verify_instrument_id_in_response(port_id, instrument_id, port_dic)

        # update local image if instrument was actually disconnected in this call:
        if instr_res == NormalResponse.INSTRUMENT_DISCONNECTED:
            del self._pnode.ports[port_id].instruments[instrument_id]
            log.debug("%r: port_id=%s disconnect_instrument: local image updated: %s",
                      self._platform_id, port_id, instrument_id)

        return dic_plat  # note: return the dic for the platform

    def get_connected_instruments(self, port_id):
        log.debug("%r: get_connected_instruments: port_id=%s",
                  self._platform_id, port_id)

        self._assert_rsn_oms()

        response = self._rsn_oms.get_connected_instruments(self._platform_id, port_id)
        log.debug("%r: port_id=%r: get_connected_instruments response: %s",
            self._platform_id, port_id, response)

        dic_plat = self._verify_platform_id_in_response(response)
        port_dic = self._verify_port_id_in_response(port_id, dic_plat)

        return dic_plat  # note: return the dic for the platform

    def turn_on_port(self, port_id):
        log.debug("%r: turning on port: port_id=%s",
                  self._platform_id, port_id)

        self._assert_rsn_oms()

        response = self._rsn_oms.turn_on_platform_port(self._platform_id, port_id)
        log.debug("%r: turn_on_platform_port response: %s",
            self._platform_id, response)

        dic_plat = self._verify_platform_id_in_response(response)
        self._verify_port_id_in_response(port_id, dic_plat)

        return dic_plat  # note: return the dic for the platform

    def turn_off_port(self, port_id):
        log.debug("%r: turning off port: port_id=%s",
                  self._platform_id, port_id)

        self._assert_rsn_oms()

        response = self._rsn_oms.turn_off_platform_port(self._platform_id, port_id)
        log.debug("%r: turn_off_platform_port response: %s",
            self._platform_id, response)

        dic_plat = self._verify_platform_id_in_response(response)
        self._verify_port_id_in_response(port_id, dic_plat)

        return dic_plat  # note: return the dic for the platform

    ###############################################
    # External event handling:

    def _register_event_listener(self, url):
        """
        Registers given url for all event types.
        """
        result = self._rsn_oms.register_event_listener(url, [])
        log.info("register_event_listener url=%r returned: %s", url, result)

    def _unregister_event_listener(self, url):
        """
        Unregisters given url for all event types.
        """
        result = self._rsn_oms.unregister_event_listener(url, [])
        log.info("unregister_event_listener url=%r returned: %s", url, result)

    def _start_event_dispatch(self):
        """
        Starts the dispatch of events received from the platform network to do
        corresponding event notifications.
        """
        self._assert_rsn_oms()

        # start http server:
        self._event_listener.start_http_server()

        # then, register my listener:
        self._register_event_listener(self._event_listener.url)

        return "OK"

    def _stop_event_dispatch(self):
        """
        Stops the dispatch of events received from the platform network.
        """
        self._assert_rsn_oms()

        # unregister my listener:
        self._unregister_event_listener(self._event_listener.url)

        # then, stop http server:
        self._event_listener.stop_http_server()

        return "OK"

    ##############################################################
    # sync/checksum
    ##############################################################

    def get_checksum(self):
        """
        Returns the checksum associated to this platform.

        @return SHA1 hash value as string of hexadecimal digits.
        """
        log.debug("%r: get_checksum...", self._platform_id)
        self._assert_rsn_oms()
        response = self._rsn_oms.get_checksum(self._platform_id)
        dic_plat = self._verify_platform_id_in_response(response)
        log.debug("%r: get_checksum... dic_plat=%s" % (self._platform_id, dic_plat))
        return dic_plat  # note: return the dic for the platform
