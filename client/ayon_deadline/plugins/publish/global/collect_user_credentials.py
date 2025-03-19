# -*- coding: utf-8 -*-
"""Collect user credentials

Requires:
    context -> project_settings
    instance.data["deadline"]["url"]

Provides:
    instance.data["deadline"] -> require_authentication (bool)
    instance.data["deadline"] -> auth (tuple (str, str)) -
        (username, password) or None
"""
import pyblish.api

from ayon_api import get_server_api_connection

from ayon_deadline.lib import FARM_FAMILIES


class CollectDeadlineUserCredentials(pyblish.api.InstancePlugin):
    """Collects user name and password for artist if DL requires authentication

    If Deadline server is marked to require authentication, it looks first for
    default values in 'Studio Settings', which could be overriden by artist
    dependent values from 'Site settings`.
    """
    order = pyblish.api.CollectorOrder + 0.250
    label = "Collect Deadline User Credentials"

    targets = ["local"]

    families = FARM_FAMILIES

    def process(self, instance):
        if not instance.data.get("farm"):
            self.log.debug("Should not be processed on farm, skipping.")
            return

        collected_deadline_url = instance.data["deadline"]["url"]
        if not collected_deadline_url:
            raise ValueError("Instance doesn't have '[deadline][url]'.")
        context_data = instance.context.data

        # deadline url might be set directly from instance, need to find
        # metadata for it
        deadline_server_name = instance.data["deadline"].get("serverName")
        dealine_info_by_server_name = {
            deadline_info["name"]: deadline_info
            for deadline_info in (
                context_data["project_settings"]["deadline"]["deadline_urls"]
            )
        }
        if deadline_server_name is None:
            self.log.warning(
                "DEV WARNING: Instance does not have set"
                " instance['deadline']['serverName']."
            )
            for deadline_info in dealine_info_by_server_name.values():
                dl_settings_url = deadline_info["value"].strip().rstrip("/")
                if dl_settings_url == collected_deadline_url:
                    deadline_server_name = deadline_info["name"]
                    break

        if not deadline_server_name:
            raise ValueError(
                f"Collected {collected_deadline_url} doesn't"
                " match any site configured in Studio Settings"
            )

        deadline_info = dealine_info_by_server_name[deadline_server_name]
        instance.data["deadline"]["require_authentication"] = (
            deadline_info["require_authentication"]
        )
        instance.data["deadline"]["auth"] = None

        instance.data["deadline"]["verify"] = (
            not deadline_info["not_verify_ssl"]
        )

        if not deadline_info["require_authentication"]:
            return

        addons_manager = instance.context.data["ayonAddonsManager"]
        deadline_addon = addons_manager["deadline"]

        default_username = deadline_info["default_username"]
        default_password = deadline_info["default_password"]
        if default_username and default_password:
            self.log.debug("Setting credentials from defaults")
            instance.data["deadline"]["auth"] = (
                default_username, default_password
            )

        # TODO import 'get_addon_site_settings' when available
        #   in public 'ayon_api'
        local_settings = get_server_api_connection().get_addon_site_settings(
            deadline_addon.name, deadline_addon.version)
        local_settings = local_settings["local_settings"]
        for server_info in local_settings:
            if deadline_server_name == server_info["server_name"]:
                if server_info["username"] and server_info["password"]:
                    self.log.debug("Setting credentials from Site Settings")
                    instance.data["deadline"]["auth"] = \
                        (server_info["username"], server_info["password"])
                    break
