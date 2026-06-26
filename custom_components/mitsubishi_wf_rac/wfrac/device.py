"""Device module"""
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any
import logging

from async_timeout import timeout
from homeassistant.core import HomeAssistant
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed
from homeassistant.util import Throttle

from .rac_parser import RacParser
from .repository import Repository
from .models.aircon import Aircon, AirconStat

from ..const import (
    CONTROL_DEBOUNCE_PERIOD,
    CONTROL_STATUS_REFRESH_DELAY,
    DOMAIN,
    MIN_TIME_BETWEEN_UPDATES,
)

_LOGGER = logging.getLogger(__name__)

class Device(DataUpdateCoordinator):  # pylint: disable=too-many-instance-attributes
    """Device Class"""

    def __init__(  # pylint: disable=too-many-arguments
            self,
            hass: HomeAssistant,
            name: str,
            hostname: str,
            port: int,
            device_id: str,
            operator_id: str,
            airco_id: str,
            availability_retry_limit: int,
            status_update_interval: timedelta,
            create_swing_mode_select: bool,
            log_http_calls: bool,
    ) -> None:
        self._api = Repository(
            hass,
            hostname,
            port,
            operator_id,
            device_id,
            log_http_calls,
        )
        self._parser = RacParser()
        self._hass = hass

        # Protected state
        self._airco = Aircon()
        self._operator_id = operator_id
        self._device_id = device_id
        self._host = hostname
        self._port = port
        self._airco_id = airco_id
        self._available = False
        self._name = name
        self._firmware = ""
        self._connected_accounts = -1
        self._availability_error_count = 0
        self._availability_retry_limit = availability_retry_limit
        self._status_update_interval = status_update_interval
        self._last_status_update: datetime | None = None
        self._pending_airco_params: dict[str, Any] = {}
        self._set_airco_task: asyncio.Task | None = None
        self._status_refresh_task: asyncio.Task | None = None
        self._airco_command_version = 0
        self._airco_command_in_flight = False
        self._create_swing_mode_select = create_swing_mode_select

        super().__init__(
            hass,
            _LOGGER,
            name=name,
            update_interval=status_update_interval,
        )

    @Throttle(MIN_TIME_BETWEEN_UPDATES)
    async def update(self):
        """Update the device information from API"""
        await self._update_status_from_api()

    def _has_pending_airco_command(self) -> bool:
        """Return true while an optimistic command is pending or being sent."""
        return (
            bool(self._pending_airco_params)
            or (
                self._set_airco_task is not None
                and not self._set_airco_task.done()
            )
            or self._airco_command_in_flight
        )

    async def _update_status_from_api(self, force: bool = False) -> None:
        """Update the device information from API."""
        if not force and self._has_pending_airco_command():
            _LOGGER.debug(
                "Skipping status update for [%s]; airco command is pending",
                self.device_name,
            )
            return

        now = datetime.now(timezone.utc)
        if not force and (
            self._last_status_update is not None
            and now - self._last_status_update < self._status_update_interval
        ):
            return
        self._last_status_update = now

        try:
            response = await self._api.get_aircon_stats()

            if response is None:
                self._record_availability_failure("status response was empty")
                _LOGGER.warning("Received no data for device %s", self._airco_id)
                return
        except Exception as ex:  # pylint: disable=broad-except
            self._record_availability_failure(
                f"{type(ex).__name__}: {ex}"
            )
            _LOGGER.warning(
                "Could not update airco [%s] status from %s:%s: %s: %s",
                self.device_name,
                self.host,
                self.port,
                type(ex).__name__,
                ex,
            )
            _LOGGER.debug(
                "Detailed exception while updating airco [%s]",
                self.device_name,
                exc_info=True,
            )
            return

        try:
            self._connected_accounts = int(response["numOfAccount"])
            self._firmware = f'{response["firmType"]}, mcu: {response["mcu"]["firmVer"]}, wireless: {response["wireless"]["firmVer"]}'
            self._airco = self._parser.translate_bytes(response["airconStat"])
            self._mark_device_access_successful()
            self.async_set_updated_data(self._airco)
        except Exception as e:  # pylint: disable=broad-except
            _LOGGER.warning(
                "Could not parse airco [%s] status response from %s:%s: %s: %s",
                self.device_name,
                self.host,
                self.port,
                type(e).__name__,
                e,
            )
            _LOGGER.debug(
                "Airco [%s] response keys before parse failure: %s",
                self.device_name,
                list(response.keys()) if isinstance(response, dict) else type(response).__name__,
            )
            _LOGGER.debug(
                "Detailed exception while parsing airco [%s] status response",
                self.device_name,
                exc_info=True,
            )
            self._record_availability_failure(
                f"status response parse failed: {type(e).__name__}: {e}"
            )

    async def delete_account(self):
        """Delete account (operator id) from the airco"""
        try:
            result = await self._api.del_account_info(self._airco_id)
            self._mark_device_access_successful()
            return result
        except Exception:  # pylint: disable=broad-except
            _LOGGER.warning("Could not delete account from airco %s", self._airco_id)

    async def add_account(self):
        """Add account (operator id) from the airco"""
        try:
            result = await self._api.update_account_info(
                self._airco_id, self._hass.config.time_zone
            )
            self._mark_device_access_successful()
            return result
        except Exception:  # pylint: disable=broad-except
            _LOGGER.warning("Could not add account from airco %s", self._airco_id)

    async def set_airco(self, params: dict[str, Any]) -> None:
        """Optimistically update and debounce sending an airco command."""
        _LOGGER.debug("Queueing airco update: %s", params)
        if self.airco is None:
            await self._hass.async_add_executor_job(self.update)

        if self._airco is None:
            raise ValueError("Airco object is empty")

        self._pending_airco_params.update(params)
        self._airco_command_version += 1
        self._apply_airco_params(params)
        self.async_set_updated_data(self._airco)

        if (
            self._status_refresh_task is not None
            and not self._status_refresh_task.done()
        ):
            self._status_refresh_task.cancel()

        if self._set_airco_task is not None and not self._set_airco_task.done():
            self._set_airco_task.cancel()

        self._set_airco_task = self._hass.async_create_task(
            self._debounced_send_airco()
        )

    def _apply_airco_params(self, params: dict[str, Any]) -> None:
        """Apply pending command values to the local optimistic state."""
        for key, value in params.items():
            setattr(self._airco, key, value)

    async def _debounced_send_airco(self) -> None:
        """Send the latest queued command after the debounce period."""
        try:
            await asyncio.sleep(CONTROL_DEBOUNCE_PERIOD.total_seconds())
        except asyncio.CancelledError:
            return

        params = self._pending_airco_params.copy()
        self._pending_airco_params.clear()
        self._set_airco_task = None
        command_version = self._airco_command_version

        if not params:
            return

        await self._send_airco(params, command_version)

    async def _send_airco(self, params: dict[str, Any], command_version: int) -> None:
        """Send airco command values to the device."""
        _LOGGER.debug("Sending debounced airco update: %s", params)
        airco_stat = AirconStat(self._airco)

        try:
            self._airco_command_in_flight = True
            command = self._parser.to_base64(airco_stat)
            response = await self._api.send_airco_command(self._airco_id, command)
            self._mark_device_access_successful()
            if command_version == self._airco_command_version:
                self._airco = self._parser.translate_bytes(response)
                self.async_set_updated_data(self._airco)
            else:
                _LOGGER.debug(
                    "Ignoring stale airco response for [%s]; newer command is pending",
                    self.device_name,
                )
        except Exception as e:  # pylint: disable=broad-except
            _LOGGER.warning(
                "Could not send airco command to [%s] at %s:%s: %s: %s",
                self.device_name,
                self.host,
                self.port,
                type(e).__name__,
                e,
            )
            _LOGGER.debug(
                "Airco command failure for [%s] does not increment the status failure counter",
                self.device_name,
                exc_info=True,
            )
        finally:
            self._airco_command_in_flight = False
            self._schedule_status_refresh_after_control_delay()

    def _schedule_status_refresh_after_control_delay(self) -> None:
        """Schedule a status refresh after allowing the device to settle."""
        if (
            self._status_refresh_task is not None
            and not self._status_refresh_task.done()
        ):
            self._status_refresh_task.cancel()

        self._status_refresh_task = self._hass.async_create_task(
            self._refresh_status_after_control_delay()
        )

    async def _refresh_status_after_control_delay(self) -> None:
        """Refresh status after a command has had time to settle on the device."""
        try:
            await asyncio.sleep(CONTROL_STATUS_REFRESH_DELAY.total_seconds())
        except asyncio.CancelledError:
            return

        self._status_refresh_task = None
        if self._has_pending_airco_command():
            _LOGGER.debug(
                "Skipping delayed status refresh for [%s]; newer command is pending",
                self.device_name,
            )
            return

        _LOGGER.debug(
            "Refreshing status for [%s] after control command settle delay",
            self.device_name,
        )
        await self._update_status_from_api(force=True)

    def _mark_device_access_successful(self) -> None:
        """Reset availability failures after successful device communication."""
        if self._availability_error_count:
            _LOGGER.debug(
                "Reset availability failure counter for [%s] after successful device access; previous count was %s",
                self.device_name,
                self._availability_error_count,
            )
        self._availability_error_count = 0
        self._available = True

    def _record_availability_failure(self, reason: str) -> None:
        """Record a failed status update without marking unavailable too early."""
        self._availability_error_count += 1
        if self._availability_error_count <= self._availability_retry_limit:
            _LOGGER.debug(
                "Ignoring failed status request for [%s] because availability failure count %s/%s has not exceeded the retry limit; reason: %s",
                self.device_name,
                self._availability_error_count,
                self._availability_retry_limit,
                reason,
            )
            return

        _LOGGER.debug(
            "Marking [%s] unavailable after %s consecutive availability failures; retry limit is %s; reason: %s",
            self.device_name,
            self._availability_error_count,
            self._availability_retry_limit,
            reason,
        )
        self._available = False

    def set_available(self, available: bool):
        """Set available status"""
        if available:
            self._mark_device_access_successful()
        else:
            self._available = False

    @property
    def device_info(self) -> DeviceInfo:
        """Return a device description for device registry."""
        return {
            "sw_version": self._firmware,
            "identifiers": {(DOMAIN, self.airco_id)},
            "manufacturer": "Mitsubishi (WF-RAC)",
            # "model": self.airco.ModelNr,
            "name": self.device_name,
        }

    @property
    def operator_id(self) -> str:
        """Return Airco Operator ID"""
        return self._operator_id

    @property
    def num_accounts(self) -> int:
        """Return Accounts connected"""
        return self._connected_accounts

    @property
    def device_id(self) -> str:
        """Return Airco device ID"""
        return self._device_id

    @property
    def host(self) -> str:
        """Get Host (IP)"""
        return self._host

    @property
    def port(self) -> int:
        """Get Port"""
        return self._port

    @property
    def device_name(self) -> str:
        """Get given Airco name"""
        return self._name

    @property
    def airco_id(self) -> str:
        """Return Airco ID"""
        return self._airco_id

    @property
    def airco(self) -> Aircon:
        """Return parsed Aircon object if set otherwise None"""
        return self._airco

    @property
    def available(self) -> bool:
        """Return True if device is available"""
        return self._available

    @property
    def create_swing_mode_select(self) -> bool:
        """Create swing mode select"""
        return self._create_swing_mode_select

    async def _async_update_data(self):
        """Update data via library."""
        try:
            async with timeout(10):
                await asyncio.gather(*[self.update()])
        except Exception as error:
            raise UpdateFailed(error) from error
