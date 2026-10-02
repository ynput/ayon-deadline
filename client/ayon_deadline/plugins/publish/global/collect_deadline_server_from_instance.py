# -*- coding: utf-8 -*-
"""Collect Deadline server information from instance.

This module collects Deadline Webservice name and URL for instance.
Based on data stored on instance a deadline information is stored to instance
data.

TODOS:
- Don't store deadline url, but use server name instead.
"""
from typing import Optional, Tuple

import pyblish.api
from ayon_core.pipeline.publish import PublishError

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

        deadline_url = deadline_info.get("url")
        server_name = deadline_info.get("serverName")

        if not deadline_url:
            context_deadline_info = instance.context.data["deadline"]
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
        for server_info in deadline_settings["deadline_urls"]:
            if server_info["value"].strip().rstrip("/") == deadline_url:
                return server_info["name"]
        return None