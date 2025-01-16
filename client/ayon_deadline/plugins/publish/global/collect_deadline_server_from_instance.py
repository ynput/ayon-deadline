# -*- coding: utf-8 -*-
"""Collect Deadline server information from instance.

This module collects Deadline Webservice name and URL for instance.
Based on data stored on instance a deadline information is stored to instance
data.

For maya this is resolving index of server lists stored in `deadlineServers`
instance attribute or using default server if that attribute doesn't exists.
That happens for backwards compatibility and should be removed in future
releases.

TODOS:
- Remove backwards compatibility for `deadlineServers` attribute.
- Remove backwards compatibility for `deadlineUrl` attribute.
- Don't store deadline url, but use server name instead.

"""
from typing import Optional, Tuple

import pyblish.api
from ayon_core.pipeline.publish import KnownPublishError, PublishError

from ayon_deadline.lib import FARM_FAMILIES


class CollectDeadlineServerFromInstance(pyblish.api.InstancePlugin):
    """Collect Deadline Webservice URL from instance."""

    # Run before collect_render.
    order = pyblish.api.CollectorOrder + 0.225
    label = "Deadline Webservice from the Instance"
    targets = ["local"]

    families = FARM_FAMILIES

    def process(self, instance):
        if not instance.data.get("farm"):
            self.log.debug("Should not be processed on farm, skipping.")
            return

        # NOTE: Remove when nothing sets 'deadline' to 'None'
        if not instance.data.get("deadline"):
            # reset if key is None or not available
            instance.data["deadline"] = {}
        deadline_info = instance.data["deadline"]

        context = instance.context
        host_name = context.data["hostName"]
        # TODO: Host specific logic should be avoided
        #   - all hosts should have same data structure on instances
        server_name = None
        if host_name == "maya":
            deadline_url, server_name = self._collect_maya_deadline_server(
                instance
            )
        else:
            # TODO remove backwards compatibility
            deadline_url = instance.data.get("deadlineUrl")
            if not deadline_url:
                deadline_url = deadline_info.get("url")
                server_name = deadline_info.get("serverName")

        if not deadline_url:
            context_deadline_info = context.data["deadline"]
            deadline_url = context_deadline_info["defaultUrl"]
            server_name = context_deadline_info["defaultServerName"]

        if not server_name:
            server_name = self._find_server_name(instance, deadline_url)

        if not server_name:
            raise PublishError(
                f"Collected deadline URL '{deadline_url}' does not match any"
                f" existing deadline servers configured in Studio Settings."
            )

        deadline_url = deadline_url.strip().rstrip("/")
        deadline_info["url"] = deadline_url
        # TODO prefer server name over url
        deadline_info["serverName"] = server_name

        self.log.debug(
            f"Server '{server_name}' ({deadline_url})"
            " will be used for submission."
        )

    def _find_server_name(
        self, instance: pyblish.api.Instance,
        deadline_url: str,
    ) -> Optional[str]:
        """Find server name from project settings based on url.

        Args:
            instance (pyblish.api.Instance): Instance object.
            deadline_url (str): Deadline Webservice URL.

        Returns:
            Optional[str]: Deadline server name.

        """
        deadline_url = deadline_url.strip().rstrip("/")

        deadline_settings = (
            instance.context.data["project_settings"]["deadline"]
        )
        for server_info in deadline_settings["deadline_servers_info"]:
            if server_info["value"].strip().rstrip("/") == deadline_url:
                return server_info["name"]
        return None

    def _collect_maya_deadline_server(
        self, render_instance: pyblish.api.Instance
    ) -> Tuple[str, str]:
        """Get Deadline Webservice URL from render instance.

        This will get all configured Deadline Webservice URLs and create
        subset of them based upon project configuration. It will then take
        `deadlineServers` from render instance that is now basically `int`
        index of that list.

        Args:
            render_instance (pyblish.api.Instance): Render instance created
                by Creator in Maya.

        Returns:
            tuple[str, str]: Selected Deadline Webservice URL.

        """
        from maya import cmds

        deadline_settings = (
            render_instance.context.data
            ["project_settings"]
            ["deadline"]
        )
        # QUESTION How and where is this is set? Should be removed?
        instance_server = render_instance.data.get("deadlineServers")
        if not instance_server:
            context_deadline_info = render_instance.context.data["deadline"]
            default_server_url = context_deadline_info["defaultUrl"]
            default_server_name = context_deadline_info["defaultServerName"]
            self.log.debug("Using default server.")
            return default_server_url, default_server_name

        # Get instance server as sting.
        if isinstance(instance_server, int):
            instance_server = cmds.getAttr(
                "{}.deadlineServers".format(render_instance.data["objset"]),
                asString=True
            )

        default_servers = {
            url_item["name"]: url_item["value"]
            for url_item in deadline_settings["deadline_servers_info"]
        }
        project_servers = deadline_settings["deadline_servers"]
        if not project_servers:
            self.log.debug("Not project servers found. Using default servers.")
            return default_servers[instance_server], instance_server

        # TODO create validation plugin for this check
        project_enabled_servers = {
            k: default_servers[k]
            for k in project_servers
            if k in default_servers
        }
        if instance_server not in project_enabled_servers:
            msg = (
                "\"{}\" server on instance is not enabled in project settings."
                " Enabled project servers:\n{}".format(
                    instance_server, project_enabled_servers
                )
            )
            raise KnownPublishError(msg)

        self.log.debug("Using project approved server.")
        return project_enabled_servers[instance_server], instance_server
