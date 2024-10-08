# -*- coding: utf-8 -*-
"""Collect Deadline servers from instance.

This is resolving index of server lists stored in `deadlineServers` instance
attribute or using default server if that attribute doesn't exists.

"""
import os

import pyblish.api

from ayon_deadline.lib import FARM_FAMILIES, JOB_ENV_DATA_KEY


class CollectDeadlineJobEnvVars(pyblish.api.ContextPlugin):
    """Collect set of environment variables to submit with deadline jobs"""
    order = pyblish.api.CollectorOrder
    label = "Deadline Farm Environment Variables"
    families = FARM_FAMILIES
    targets = ["local"]

    ENV_KEYS = [
        # AYON
        "AYON_BUNDLE_NAME",
        "AYON_DEFAULT_SETTINGS_VARIANT",
        "AYON_PROJECT_NAME",
        "AYON_FOLDER_PATH",
        "AYON_TASK_NAME",
        "AYON_APP_NAME",
        "AYON_WORKDIR",
        "AYON_APP_NAME",
        "AYON_LOG_NO_COLORS",
        "AYON_IN_TESTS",
        "IS_TEST",  # backwards compatibility

        # Ftrack
        "FTRACK_API_KEY",
        "FTRACK_API_USER",
        "FTRACK_SERVER",
        "PYBLISHPLUGINPATH",

        # Shotgrid
        "OPENPYPE_SG_USER",
    ]

    def process(self, context):
        env = {}
        for key in self.ENV_KEYS:
            value = os.getenv(key)
            if value:
                self.log.debug(f"Setting job env: {key}: {value}")
                env[key] = value

        # Transfer some environment variables from current context
        context.data.setdefault(JOB_ENV_DATA_KEY, {}).update(env)
