import os
import subprocess
import typing
from typing import Optional, List, Dict, Any, Tuple

import requests
import ayon_api

from ayon_core.lib import get_ayon_username
from ayon_core.addon import AYONAddon, IPluginPaths

from .version import __version__
from .constants import AYON_PLUGIN_VERSION
from .lib import (
    JobType,
    DeadlineServerInfo,
    get_deadline_workers,
    get_deadline_groups,
    get_deadline_limit_groups,
    get_deadline_pools,
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
        job_info: Dict[str, Any],
        aux_files: Optional[List[str]] = None,
    ) -> str:
        """Submit job to Deadline.

        Args:
            server_name (str): Deadline Server name from project Settings.
            plugin_info (dict): Plugin info data.
            job_info (dict): Job info data.
            aux_files (Optional[List[str]]): List of auxiliary files.

        Returns:
            str: Job ID.

        """
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
        return response.json()["_id"]

    def submit_ayon_plugin_job(
        self,
        server_name: str,
        args: "Union[List[str], str]",
        job_name: str,
        batch_name: str,
        job_type: Optional[JobType] = None,
        department: Optional[str] = None,
        chunk_size: Optional[int] = 1,
        priority: Optional[int] = 50,
        initial_status: Optional["InitialStatus"] = "Active",
        group: Optional[str] = None,
        pool: Optional[str] = None,
        secondary_pool: Optional[str] = None,
        username: Optional[str] = None,
        comment: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
        dependency_job_ids: Optional[List[str]] = None,
        custom_job_info: Optional[Dict[str, Any]] = None,
        aux_files: Optional[List[str]] = None,
    ) -> str:
        if chunk_size is None:
            chunk_size = 1

        if priority is None:
            priority = 50

        if priority < 0 or priority > 100:
            raise ValueError("Priority must be between 0-100")

        if initial_status is None:
            initial_status = "Active"

        if initial_status not in ["Active", "Suspended"]:
            raise ValueError(
                "InitialStatus must be one of"
                " 'Active', 'Suspended'"
            )

        if username is None:
            username = get_ayon_username()

        if dependency_job_ids is None:
            dependency_job_ids = []

        if env is None:
            env = {}

        if job_type is None:
            job_type = JobType.UNDEFINED

        env.update(job_type.get_job_env())

        job_info = {
            "Plugin": "Ayon",
            "BatchName": batch_name,
            "Name": job_name,
            "UserName": username,

            "ChunkSize": chunk_size,
            "Priority": priority,
            "InitialStatus": initial_status,
        }
        for key, value in (
            ("Comment", comment),
            ("Department", department),
            ("Group", group),
            ("Pool", pool),
            ("SecondaryPool", secondary_pool),
        ):
            if value is not None:
                job_info[key] = value

        for idx, job_id in enumerate(dependency_job_ids):
            job_info[f"JobDependency{idx}"] = job_id

        for idx, (key, value) in enumerate(env.items()):
            info_key = f"EnvironmentKeyValue{idx}"
            job_info[info_key] = f"{key}={value}"

        if custom_job_info is not None:
            job_info.update(custom_job_info)

        if isinstance(args, str):
            command = args
        else:
            command = subprocess.list2cmdline(args)

        plugin_info = {
            "Version": AYON_PLUGIN_VERSION,
            "Arguments": command,
            "SingleFrameOnly": "True",
        }
        return self.submit_job(server_name, plugin_info, job_info, aux_files)

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
