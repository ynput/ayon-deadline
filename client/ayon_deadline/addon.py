import os
from typing import Optional, List, Dict, Any, Tuple

import ayon_api

from ayon_core.addon import AYONAddon, IPluginPaths

from .lib import (
    DeadlineServerInfo,
    get_deadline_workers,
    get_deadline_groups,
    get_deadline_limit_groups,
    get_deadline_pools,
)
from .version import __version__


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
                pools=pools,
                limit_groups=limit_groups,
                groups=groups,
                machines=machines
            )
            self._server_info_by_name[server_name] = server_info

        return server_info

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
