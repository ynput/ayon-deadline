import os
import sys

import requests
import six
from typing import Optional, List, Tuple

from ayon_core.lib import Logger
from ayon_core.addon import AYONAddon, IPluginPaths

from .version import __version__


DEADLINE_ADDON_ROOT = os.path.dirname(os.path.abspath(__file__))


class DeadlineWebserviceError(Exception):
    """
    Exception to throw when connection to Deadline server fails.
    """


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

    @classmethod
    def get_deadline_pools(
        cls,
        webservice_url: str,
        auth: Optional[Tuple[str, str]] = None,
        log: Optional[Logger] = None
    ) -> List[str]:
        """Get pools from Deadline API.

        Args:
            webservice_url (str): Server url.
            auth (Optional[Tuple[str, str]]): Tuple containing username,
                password
            log (Optional[Logger]): Logger to log errors to, if provided.

        Returns:
            List[str]: Limit Groups.

        Raises:
            RuntimeError: If deadline webservice is unreachable.

        """
        endpoint = "{}/api/pools?NamesOnly=true".format(webservice_url)
        return DeadlineAddon._get_deadline_info(
            endpoint, auth, log, item_type="pools")

    @classmethod
    def get_deadline_groups(
        cls,
        webservice_url: str,
        auth: Optional[Tuple[str, str]] = None,
        log: Optional[Logger] = None
    ) -> List[str]:
        """Get Groups from Deadline API.

        Args:
            webservice_url (str): Server url.
            auth (Optional[Tuple[str, str]]): Tuple containing username,
                password
            log (Optional[Logger]): Logger to log errors to, if provided.

        Returns:
            List[str]: Limit Groups.

        Raises:
            RuntimeError: If deadline webservice_url is unreachable.

        """
        endpoint = "{}/api/groups".format(webservice_url)
        return DeadlineAddon._get_deadline_info(
            endpoint, auth, log, item_type="groups")

    @classmethod
    def get_deadline_limit_groups(
        cls,
        webservice_url: str,
        auth: Optional[Tuple[str, str]] = None,
        log: Optional[Logger] = None
    ) -> List[str]:
        """Get Limit Groups from Deadline API.

        Args:
            webservice_url (str): Server url.
            auth (Optional[Tuple[str, str]]): Tuple containing username,
                password
            log (Optional[Logger]): Logger to log errors to, if provided.

        Returns:
            List[str]: Limit Groups.

        Raises:
            RuntimeError: If deadline webservice_url is unreachable.

        """
        endpoint = "{}/api/limitgroups?NamesOnly=true".format(webservice_url)
        return DeadlineAddon._get_deadline_info(
            endpoint, auth, log, item_type="limitgroups")

    @classmethod
    def get_deadline_workers(
        cls,
        webservice_url: str,
        auth: Optional[Tuple[str, str]] = None,
        log: Optional[Logger] = None
    ) -> List[str]:
        """Get Workers (eg.machine names) from Deadline API.

        Args:
            webservice_url (str): Server url.
            auth (Optional[Tuple[str, str]]): Tuple containing username,
                password
            log (Optional[Logger]): Logger to log errors to, if provided.

        Returns:
            List[str]: Limit Groups.

        Raises:
            RuntimeError: If deadline webservice_url is unreachable.

        """
        endpoint = "{}/api/slaves?NamesOnly=true".format(webservice_url)
        return DeadlineAddon._get_deadline_info(
            endpoint, auth, log, item_type="workers")

    @classmethod
    def _get_deadline_info(
        cls,
        endpoint,
        auth=None,
        log=None,
        item_type=None
    ):
        from .abstract_submit_deadline import requests_get

        if not log:
            log = Logger.get_logger(__name__)

        try:
            kwargs = {}
            if auth:
                kwargs["auth"] = auth
            response = requests_get(endpoint, **kwargs)
        except requests.exceptions.ConnectionError as exc:
            msg = 'Cannot connect to DL web service {}'.format(endpoint)
            log.error(msg)
            six.reraise(
                DeadlineWebserviceError,
                DeadlineWebserviceError('{} - {}'.format(msg, exc)),
                sys.exc_info()[2])
        if not response.ok:
            log.warning(f"No {item_type} retrieved")
            return []

        return response.json()

    def get_pools_by_server_name(self, server_name: str) -> List[str]:
        """Returns dictionary of pools per DL server

        Args:
            server_name (str): Deadline Server name from Project Settings.

        Returns:
            Dict[str, List[str]]: {"default": ["pool1", "pool2"]}

        """
        pools = self._pools_by_server_name.get(server_name)
        if pools is None:
            dl_server_info = self.deadline_servers_info.get(server_name)

            auth = (dl_server_info["default_username"],
                    dl_server_info["default_password"])
            pools = self.get_deadline_pools(
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
            dl_server_info = self.deadline_servers_info.get(server_name)

            auth = (dl_server_info["default_username"],
                    dl_server_info["default_password"])
            groups = self.get_deadline_groups(
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
            dl_server_info = self.deadline_servers_info.get(server_name)

            auth = (dl_server_info["default_username"],
                    dl_server_info["default_password"])
            limit_groups = self.get_deadline_limit_groups(
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
            dl_server_info = self.deadline_servers_info.get(server_name)

            auth = (dl_server_info["default_username"],
                    dl_server_info["default_password"])
            machines = self.get_deadline_workers(
                dl_server_info["value"],
                auth
            )
            self._machines_by_server_name[server_name] = machines

        return machines
