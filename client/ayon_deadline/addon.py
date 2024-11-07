import os

from typing import Optional, List

from ayon_core.addon import AYONAddon, IPluginPaths

from .lib import (
    get_deadline_workers,
    get_deadline_groups,
    get_deadline_limit_groups,
    get_deadline_pools
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

        self._pools_by_server_name = {}
        self._limit_groups_by_server_name = {}
        self._groups_by_server_name = {}
        self._machines_by_server_name = {}

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

    def get_pools_by_server_name(self, server_name: str) -> List[str]:
        """Returns dictionary of pools per DL server

        Args:
            server_name (str): Deadline Server name from Project Settings.

        Returns:
            Dict[str, List[str]]: {"default": ["pool1", "pool2"]}

        """
        pools = self._pools_by_server_name.get(server_name)
        if pools is None:
            dl_server_info = self.deadline_servers_info[server_name]

            auth = (dl_server_info["default_username"],
                    dl_server_info["default_password"])
            pools = get_deadline_pools(
                dl_server_info["value"],
                auth
            )
            self._pools_by_server_name[server_name] = pools

        return pools

    def get_groups_by_server_name(self, server_name: str) -> List[str]:
        """Returns dictionary of groups per DL server

        Args:
            server_name (str): Deadline Server name from Project Settings.

        Returns:
            Dict[str, List[str]]: {"default": ["group1", "group2"]}

        """
        groups = self._groups_by_server_name.get(server_name)
        if groups is None:
            dl_server_info = self.deadline_servers_info[server_name]

            auth = (dl_server_info["default_username"],
                    dl_server_info["default_password"])
            groups = get_deadline_groups(
                dl_server_info["value"],
                auth
            )
            self._groups_by_server_name[server_name] = groups

        return groups

    def get_limit_groups_by_server_name(self, server_name: str) -> List[str]:
        """Returns dictionary of limit groups per DL server

        Args:
            server_name (str): Deadline Server name from Project Settings.

        Returns:
            Dict[str, List[str]]: {"default": ["limit1", "limit2"]}

        """
        limit_groups = self._limit_groups_by_server_name.get(server_name)
        if limit_groups is None:
            dl_server_info = self.deadline_servers_info[server_name]

            auth = (dl_server_info["default_username"],
                    dl_server_info["default_password"])
            limit_groups = get_deadline_limit_groups(
                dl_server_info["value"],
                auth
            )
            self._limit_groups_by_server_name[server_name] = limit_groups

        return limit_groups

    def get_machines_by_server_nameserver(self, server_name: str) -> List[str]:
        """Returns dictionary of machines/workers per DL server

        Args:
            server_name (str): Deadline Server name from Project Settings.

        Returns:
            Dict[str, List[str]]: {"default": ["renderNode1", "PC1"]}

        """
        machines = self._machines_by_server_name.get(server_name)
        if machines is None:
            dl_server_info = self.deadline_servers_info[server_name]

            auth = (dl_server_info["default_username"],
                    dl_server_info["default_password"])
            machines = get_deadline_workers(
                dl_server_info["value"],
                auth
            )
            self._machines_by_server_name[server_name] = machines

        return machines
