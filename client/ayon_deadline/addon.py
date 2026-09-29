from __future__ import annotations

import os
import subprocess
import time
import typing
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, List, Dict, Any, Tuple

import requests
import ayon_api

from ayon_core.addon import AYONAddon, IPluginPaths
from ayon_core.lib import CacheItem

from .version import __version__
from .constants import AYON_PLUGIN_VERSION
from .lib import (
    DeadlineConnectionInfo,
    DeadlineServerInfo,
    get_deadline_workers,
    get_deadline_groups,
    get_deadline_limit_groups,
    get_deadline_pools,
    DeadlineJobInfo,
    DeadlineWebserviceError,
)

if typing.TYPE_CHECKING:
    from typing import Union, Literal

    InitialStatus = Literal["Active", "Suspended"]

DEADLINE_ADDON_ROOT = os.path.dirname(os.path.abspath(__file__))


class _ServerInfoCache:
    """Deadline server info shared by all addon objects in the process.

    New addon object is created with every 'AddonsManager', e.g. on each
    reset of publisher, so the cache can't be stored on the addon object.
    Server info is cached by connection information so changed credentials
    are not affected by the cache.
    """
    # Lifetime of successfully fetched server info in seconds
    lifetime: int = 300
    # Lifetime of failed connection, to not wait for connection timeout
    #   on each reset when Deadline webservice is not available
    failed_lifetime: int = 30
    # Expiration time with server info, or error message if connection failed
    _items: dict[
        DeadlineConnectionInfo, tuple[float, DeadlineServerInfo | str]
    ] = {}

    @classmethod
    def get(cls, con_info: DeadlineConnectionInfo) -> DeadlineServerInfo:
        item = cls._items.get(con_info)
        if item is not None:
            expire_time, value = item
            if time.time() < expire_time:
                if isinstance(value, str):
                    raise DeadlineWebserviceError(value)
                return value

        try:
            server_info = cls._fetch(con_info)
        except DeadlineWebserviceError as exc:
            cls._items[con_info] = (
                time.time() + cls.failed_lifetime, str(exc)
            )
            raise

        cls._items[con_info] = (time.time() + cls.lifetime, server_info)
        return server_info

    @staticmethod
    def _fetch(con_info: DeadlineConnectionInfo) -> DeadlineServerInfo:
        """Fetch server info from webservice for single Deadline server.

        This uses a thread pool to quickly acquire all the information from the
        webservice, as each request can take a while.
        """
        funcs = (
            get_deadline_pools,
            get_deadline_groups,
            get_deadline_limit_groups,
            get_deadline_workers,
        )
        # Query the endpoints in parallel as each request can take a while
        with ThreadPoolExecutor(max_workers=len(funcs)) as executor:
            futures = [
                executor.submit(
                    func, con_info.url, con_info.auth, con_info.verify
                )
                for func in funcs
            ]
            pools, groups, limit_groups, machines = [
                future.result()
                for future in futures
            ]
        return DeadlineServerInfo(
            pools=pools,
            limit_groups=limit_groups,
            groups=groups,
            machines=machines
        )


class DeadlineAddon(AYONAddon, IPluginPaths):
    name = "deadline"
    version = __version__

    def initialize(self, studio_settings):
        deadline_settings = studio_settings[self.name]
        deadline_servers_info = {
            url_item["name"]: url_item
            for url_item in deadline_settings["deadline_urls"]
        }

        if not deadline_servers_info:
            self.enabled = False
            self.log.warning((
                "Deadline Webservice URLs are not specified. Disabling addon."
            ))

        self.deadline_servers_info = deadline_servers_info

        self._local_settings_cache = CacheItem(lifetime=60)

    def get_plugin_paths(self):
        """Deadline plugin paths."""
        # Note: We are not returning `publish` key because we have overridden
        # `get_publish_plugin_paths` to return paths host-specific. However,
        # `get_plugin_paths` still needs to be implemented because it's
        # abstract on the parent class
        return {}

    def get_publish_plugin_paths(
        self,
        host_name: Optional[str] = None
    ) -> List[str]:
        publish_dir = os.path.join(DEADLINE_ADDON_ROOT, "plugins", "publish")
        paths = [os.path.join(publish_dir, "global")]
        if host_name:
            paths.append(os.path.join(publish_dir, host_name))
        return paths

    def get_server_info_by_name(
        self,
        server_name: str,
        local_settings: Optional[Dict[str, Any]] = None,
    ) -> DeadlineServerInfo:
        """Returns Deadline server info by name.

        Args:
            server_name (str): Deadline Server name from Project Settings.
            local_settings (Optional[Dict[str, Any]]): Deadline local
                settings.

        Returns:
            DeadlineServerInfo: Deadline server info.

        Raises:
            DeadlineWebserviceError: When Deadline webservice is not
                available.

        """
        con_info = self.get_deadline_server_connection_info(
            server_name, local_settings
        )
        return _ServerInfoCache.get(con_info)

    def get_job_info(
        self,
        server_name: str,
        job_id: str,
        local_settings: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Get job info from Deadline.

        Args:
            server_name (str): Deadline Server name from project Settings.
            job_id (str): Deadline job id.
            local_settings (Optional[Dict[str, Any]]): Deadline local
                settings.

        Returns:
            Optional[Dict[str, Any]]: Job info from Deadline.

        """
        con_info = self.get_deadline_server_connection_info(
            server_name, local_settings
        )
        response = requests.get(
            f"{con_info.url}/api/jobs?JobID={job_id}",
            auth=con_info.auth,
            verify=con_info.verify
        )
        response.raise_for_status()
        data = response.json()
        if data:
            return data.pop(0)
        return None

    def submit_job(
        self,
        server_name: str,
        plugin_info: Dict[str, Any],
        job_info: "Union[DeadlineJobInfo, Dict[str, Any]]",
        aux_files: Optional[List[str]] = None,
        local_settings: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Submit job to Deadline.

        Args:
            server_name (str): Deadline Server name from project Settings.
            plugin_info (dict): Plugin info data.
            job_info (Union[DeadlineJobInfo, Dict[str, Any]]): Job info data.
            aux_files (Optional[List[str]]): List of auxiliary files.
            local_settings (Optional[Dict[str, Any]]): Deadline local
                settings.

        Returns:
            Dict[str, Any]: Job payload, with 'response' key containing
                response data.

        """
        if isinstance(job_info, DeadlineJobInfo):
            job_info = job_info.serialize()

        payload = {
            "JobInfo": job_info,
            "PluginInfo": plugin_info,
            "AuxFiles": aux_files or [],
        }
        con_info = self.get_deadline_server_connection_info(
            server_name, local_settings
        )
        response = requests.post(
            f"{con_info.url}/api/jobs",
            json=payload,
            timeout=10,
            auth=con_info.auth,
            verify=con_info.verify
        )
        response.raise_for_status()
        payload["response"] = response.json()
        return payload

    def submit_ayon_plugin_job(
        self,
        server_name: str,
        args: "Union[List[str], str]",
        job_info: "Union[DeadlineJobInfo, Dict[str, Any]]",
        aux_files: Optional[List[str]] = None,
        single_frame_only: bool = True,
        local_settings: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Submit job to Deadline using Ayon plugin.

        Args:
            server_name (str): Deadline Server name from settings.
            args (Union[List[str], str]): Command line arguments.
            job_info (Union[DeadlineJobInfo, Dict[str, Any]]): Job info data.
            aux_files (Optional[List[str]]): List of auxiliary files.
            single_frame_only (bool): Submit job for single frame only.
            local_settings (Optional[Dict[str, Any]]): Deadline local
                settings.

        Returns:
            Dict[str, Any]: Job payload, with 'job_id' key.

        """
        if not isinstance(args, str):
            args = subprocess.list2cmdline(args)

        if isinstance(job_info, DeadlineJobInfo):
            job_info = job_info.serialize()
        job_info["Plugin"] = "Ayon"

        plugin_info = {
            "Version": AYON_PLUGIN_VERSION,
            "Arguments": args,
            "SingleFrameOnly": "True" if single_frame_only else "False",
        }
        return self.submit_job(
            server_name,
            plugin_info,
            job_info,
            aux_files,
            local_settings=local_settings
        )

    def get_deadline_server_connection_info(
        self,
        server_name: str,
        local_settings: Optional[Dict[str, Any]] = None,
    ) -> DeadlineConnectionInfo:
        """Get Deadline server info.

        Args:
            server_name (str): Deadline Server name from project Settings.
            local_settings (Optional[Dict[str, Any]]): Deadline local
                settings.

        Returns:
            DeadlineConnectionInfo: Server connection information with
                server url, auth and verify ssl flag.

        """
        dl_server_info = self.deadline_servers_info[server_name]
        auth = self._get_server_user_auth(dl_server_info, local_settings)
        return DeadlineConnectionInfo(
            server_name,
            dl_server_info["value"].rstrip("/"),
            auth,
            not dl_server_info["not_verify_ssl"],
        )

    def _get_local_settings(self) -> Dict[str, Any]:
        if not self._local_settings_cache.is_valid:
            # TODO import 'get_addon_site_settings' when available
            #   in public 'ayon_api'
            con = ayon_api.get_server_api_connection()
            self._local_settings_cache.update_data(
                con.get_addon_site_settings(
                    self.name, self.version
                )
            )
        return self._local_settings_cache.get_data()

    def _get_server_user_auth(
        self,
        server_info: Dict[str, Any],
        local_settings: Optional[Dict[str, Any]] = None,
    ) -> Optional[Tuple[str, str]]:
        selected_server_name = server_info["name"]

        require_authentication = server_info["require_authentication"]
        if require_authentication:
            if local_settings is None:
                local_settings = self._get_local_settings()

            for local_info in local_settings["local_settings"]:
                if selected_server_name != local_info["server_name"]:
                    continue

                if local_info["username"] and local_info["password"]:
                    return local_info["username"], local_info["password"]

        default_username = server_info["default_username"]
        default_password = server_info["default_password"]
        if default_username and default_password:
            return default_username, default_password
