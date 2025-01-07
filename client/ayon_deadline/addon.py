import os
import subprocess
import typing
from typing import Optional, List, Dict, Any, Tuple

import requests
import ayon_api

from ayon_core.addon import AYONAddon, IPluginPaths

from .version import __version__
from .constants import AYON_PLUGIN_VERSION
from .lib import (
    DeadlineServerInfo,
    get_deadline_workers,
    get_deadline_groups,
    get_deadline_limit_groups,
    get_deadline_pools,
    DeadlineJobInfo,
)

if typing.TYPE_CHECKING:
    from typing import Union, Literal

    InitialStatus = Literal["Active", "Suspended"]

DEADLINE_ADDON_ROOT = os.path.dirname(os.path.abspath(__file__))


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

        self._server_info_by_name: Dict[str, DeadlineServerInfo] = {}

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

    def get_server_info_by_name(self, server_name: str) -> DeadlineServerInfo:
        """Returns Deadline server info by name.

        Args:
            server_name (str): Deadline Server name from Project Settings.

        Returns:
            DeadlineServerInfo: Deadline server info.

        """
        server_info = self._server_info_by_name.get(server_name)
        if server_info is None:
            server_url, auth, _ = self._get_deadline_con_info(server_name)
            pools = get_deadline_pools(server_url, auth)
            groups = get_deadline_groups(server_url, auth)
            limit_groups = get_deadline_limit_groups(server_url, auth)
            machines = get_deadline_workers(server_url, auth)
            server_info = DeadlineServerInfo(
                pools, groups, limit_groups, machines
            )
            self._server_info_by_name[server_name] = server_info

        return server_info

    def submit_job(
        self,
        server_name: str,
        plugin_info: Dict[str, Any],
        job_info: "Union[DeadlineJobInfo, Dict[str, Any]]",
        aux_files: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Submit job to Deadline.

        Args:
            server_name (str): Deadline Server name from project Settings.
            plugin_info (dict): Plugin info data.
            job_info (dict): Job info data.
            aux_files (Optional[List[str]]): List of auxiliary files.

        Returns:
            Dict[str, Any]: Job payload, with 'job_id' key.

        """
        if isinstance(job_info, DeadlineJobInfo):
            job_info = job_info.serialize()

        payload = {
            "JobInfo": job_info,
            "PluginInfo": plugin_info,
            "AuxFiles": aux_files or [],
        }
        server_url, auth, verify = self._get_deadline_con_info(server_name)
        response = requests.post(
            f"{server_url}/api/jobs",
            json=payload,
            timeout=10,
            auth=auth,
            verify=verify
        )
        if not response.ok:
            raise ValueError("Failed to create job")

        payload["job_id"] = response.json()["_id"]
        return payload

    def submit_ayon_plugin_job(
        self,
        server_name: str,
        args: "Union[List[str], str]",
        job_info: "Union[DeadlineJobInfo, Dict[str, Any]]",
        aux_files: Optional[List[str]] = None,
        single_frame_only: bool = True
    ) -> Dict[str, Any]:
        """Submit job to Deadline using Ayon plugin.

        Args:
            server_name (str): Deadline Server name from settings.
            args (Union[List[str], str]): Command line arguments.
            job_info (Union[DeadlineJobInfo, Dict[str, Any]]): Job info data.
            aux_files (Optional[List[str]]): List of auxiliary files.
            single_frame_only (bool): Submit job for single frame only.

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
            server_name, plugin_info, job_info, aux_files
        )

    def _get_deadline_con_info(
        self, server_name: str
    ) -> Tuple[str, Optional[Tuple[str, str]], bool]:
        dl_server_info = self.deadline_servers_info[server_name]
        auth = self._get_server_user_auth(dl_server_info)
        return (
            dl_server_info["value"],
            auth,
            not dl_server_info["not_verify_ssl"]
        )

    def _get_server_user_auth(
        self, server_info: Dict[str, Any]
    ) -> Optional[Tuple[str, str]]:
        server_name = server_info["name"]

        require_authentication = server_info["require_authentication"]
        if require_authentication:
            # TODO import 'get_addon_site_settings' when available
            #   in public 'ayon_api'
            con = ayon_api.get_server_api_connection()
            local_settings = con.get_addon_site_settings(self.name, self.version)
            local_settings = local_settings["local_settings"]
            for server_info in local_settings:
                if server_name != server_info["server_name"]:
                    continue

                if server_info["username"] and server_info["password"]:
                    return server_info["username"], server_info["password"]

        default_username = server_info["default_username"]
        default_password = server_info["default_password"]
        if default_username and default_password:
            return default_username, default_password
