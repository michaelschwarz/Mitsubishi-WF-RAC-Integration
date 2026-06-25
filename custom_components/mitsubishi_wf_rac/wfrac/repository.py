"""Local API for sending and receiving to and from WF-RAC module"""

from __future__ import annotations

import asyncio
import functools
import json
import logging
import os
import ssl
import time
from datetime import datetime, timedelta
from typing import Any

import aiohttp
from aiohttp import ClientConnectionError
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.aiohttp_client import async_get_clientsession

_LOGGER = logging.getLogger(__name__)
# log http requests/responses to separate logger, to allow easily turning on/off from
# configuration.yaml
_HTTP_LOG = _LOGGER.getChild("http")

# ensure that we don't overwhelm the aircon unit by waiting at least
# this long between successive requests
_MIN_TIME_BETWEEN_REQUESTS = timedelta(seconds=1)


class AirconApiError(HomeAssistantError):
    """Raised when the aircon API returns an error"""

    pass


class Repository:
    """Simple Api class to send and get Aircon information"""

    api_version = "1.0"

    def __init__(  # pylint: disable=too-many-arguments
        self,
        hass: HomeAssistant,
        hostname: str,
        port: int,
        operator_id: str,
        device_id: str,
        log_http_calls: bool = False,
    ) -> None:
        self._hass = hass
        self._hostname = hostname
        self._port = port
        self._operator_id = operator_id
        self._device_id = device_id
        self._session = async_get_clientsession(hass)
        self._next_request_after = datetime.now()
        self._method: str | None = None
        self._log_http_calls = log_http_calls

    def _log_http_request(self, url: str, data: dict[str, Any]) -> None:
        """Log outgoing HTTP call details when enabled for this device."""
        if not self._log_http_calls:
            return

        _HTTP_LOG.info(
            "WF-RAC HTTP request: url=%s payload=%s",
            url,
            data,
        )

    def _log_http_response(
        self,
        url: str,
        status: int,
        content_type: str | None,
        body: str,
    ) -> None:
        """Log incoming HTTP response details when enabled for this device."""
        if not self._log_http_calls:
            return

        _HTTP_LOG.info(
            "WF-RAC HTTP response: url=%s status=%s content_type=%s body=%s",
            url,
            status,
            content_type,
            body,
        )

    def _log_http_failure(self, url: str, ex: Exception) -> None:
        """Log HTTP transport failures when enabled for this device."""
        if not self._log_http_calls:
            return

        _HTTP_LOG.info(
            "WF-RAC HTTP failure: url=%s error=%s: %s",
            url,
            type(ex).__name__,
            ex,
        )

    async def _handle_response(
        self,
        command: str,
        protocol: str,
        url: str,
        resp: aiohttp.ClientResponse,
    ) -> dict[str, Any]:
        """Read, log, validate, and parse an API response."""
        body = await resp.text()
        self._log_http_response(url, resp.status, resp.content_type, body)

        if resp.status >= 400:
            raise AirconApiError(
                f"{command} via {protocol.upper()} to "
                f"{self._hostname}:{self._port} failed with HTTP "
                f"{resp.status}: {resp.reason}; body={body}"
            )

        try:
            return json.loads(body)
        except ValueError as ex:
            raise AirconApiError(
                f"{command} via {protocol.upper()} to "
                f"{self._hostname}:{self._port} returned invalid JSON: {ex}; "
                f"body={body}"
            ) from ex

    async def _post(
        self, command: str, contents: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        async def _execute_request(protocol: str) -> dict[str, Any]:
            """Executes a single POST request and returns the JSON response."""
            url = f"{protocol}://{self._hostname}:{self._port}/beaver/command/{command}"
            self._log_http_request(url, data)
            try:
                if protocol == "http":
                    async with self._session.post(
                        url, json=data, timeout=aiohttp.ClientTimeout(total=30)
                    ) as resp:
                        return await self._handle_response(
                            command,
                            protocol,
                            url,
                            resp,
                        )
                elif protocol == "https":
                    # TODO: add this logic to the config flow and try to fetch HTTPS cert automaticly
                    # If a certificate file is present, use it for SSL, otherwise bypass
                    # A certificate file can be stored in the HA configuration directory by running this command while in that directory:
                    # openssl s_client -connect <<AC_IP_ADDRESS>>:51443 -showcerts </dev/null 2>/dev/null | openssl x509 -outform PEM > ac_cert.pem
                    cert_path = "/config/ac_cert.pem"
                    cert_exists = await self._hass.async_add_executor_job(
                        os.path.isfile, cert_path
                    )

                    if cert_exists:
                        _LOGGER.debug(
                            "Certificate file found, creating secure SSL context"
                        )
                        partial_func = functools.partial(
                            ssl.create_default_context, cafile=cert_path
                        )
                        ssl_context = await self._hass.async_add_executor_job(
                            partial_func
                        )
                        ssl_context.check_hostname = False
                        connector = aiohttp.TCPConnector(ssl=ssl_context)
                    else:
                        _LOGGER.debug(
                            "Certificate file not found, falling back to insecure SSL"
                        )
                        connector = aiohttp.TCPConnector(ssl=False)

                    async with aiohttp.ClientSession(
                        connector=connector
                    ) as https_session:
                        async with https_session.post(
                            url, json=data, timeout=aiohttp.ClientTimeout(total=30)
                        ) as resp:
                            return await self._handle_response(
                                command,
                                protocol,
                                url,
                                resp,
                            )
            except (ClientConnectionError, asyncio.TimeoutError) as ex:
                self._log_http_failure(url, ex)
                raise AirconApiError(
                    f"{command} via {protocol.upper()} to "
                    f"{self._hostname}:{self._port} failed: "
                    f"{type(ex).__name__}: {ex}"
                ) from ex
            except aiohttp.ClientError as ex:
                self._log_http_failure(url, ex)
                raise AirconApiError(
                    f"{command} via {protocol.upper()} to "
                    f"{self._hostname}:{self._port} failed: "
                    f"{type(ex).__name__}: {ex}"
                ) from ex

            raise AirconApiError(f"Invalid protocol specified: {protocol}")

        data = {
            "apiVer": self.api_version,
            "command": command,
            "deviceId": self._device_id,  # is unique device ID (on android it is called android_id)
            "operatorId": self._operator_id,  # is generated UUID
            "timestamp": round(time.time()),
        }
        if contents is not None:
            data["contents"] = contents

        # ensure only one thread is talking to the device at a time
        wait_for = (self._next_request_after - datetime.now()).total_seconds()
        if wait_for > 0:
            _LOGGER.debug("Waiting for %rs until we can send a request", wait_for)
            await asyncio.sleep(wait_for)

        json_response = None

        # If we already know how to communicate with the unit, proceed
        if self._method in ("http", "https"):
            json_response = await _execute_request(self._method)

        # If we haven't yet determined if https is required, find out
        else:
            _LOGGER.debug("No stored method; attempting discovery...")
            try:
                json_response = await _execute_request("http")
                _LOGGER.info("Discovered working communication method: HTTP")
                # Store the required communication method
                self._method = "http"
            except AirconApiError:
                _LOGGER.debug("HTTP failed, trying HTTPS")
                json_response = await _execute_request("https")
                _LOGGER.info("Discovered working communication method: HTTPS")
                # Store the required communication method
                self._method = "https"

        self._next_request_after = datetime.now() + _MIN_TIME_BETWEEN_REQUESTS
        return json_response

    async def get_info(self) -> dict:
        """Simple command to get aircon details"""
        return (await self._post("getDeviceInfo"))["contents"]

    async def get_airco_id(self) -> str:
        """Simple command to get aircon ID"""
        return (await self.get_info())["airconId"]

    async def update_account_info(
        self, airco_id: str, time_zone: str
    ) -> dict[str, Any]:
        """Update the account info on the airco (sets to operator id of the device)"""
        contents = {
            "accountId": self._operator_id,
            "airconId": airco_id,
            "remote": 0,
            "timezone": time_zone,
        }
        return await self._post("updateAccountInfo", contents)

    async def del_account_info(self, airco_id: str) -> dict:
        """delete the account info on the airco"""
        contents = {"accountId": self._operator_id, "airconId": airco_id}
        return await self._post("deleteAccountInfo", contents)

    async def get_aircon_stats(self, raw=False) -> dict:
        """Get the Aricon Stats from the Airco"""
        result = await self._post("getAirconStat")
        return result if raw else result["contents"]

    async def send_airco_command(self, airco_id: str, command: str) -> str:
        """send command to the Airco"""
        contents = {"airconId": airco_id, "airconStat": command}
        result = await self._post("setAirconStat", contents)
        return result["contents"]["airconStat"]
