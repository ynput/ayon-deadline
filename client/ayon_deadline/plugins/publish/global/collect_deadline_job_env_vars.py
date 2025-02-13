import os

import pyblish.api

from ayon_core.pipeline.publish import FARM_JOB_ENV_DATA_KEY


class CollectDeadlineJobEnvVars(pyblish.api.ContextPlugin):
    """Collect set of environment variables to submit with deadline jobs"""
    order = pyblish.api.CollectorOrder
    label = "Deadline Farm Environment Variables"
    targets = ["local"]

    ENV_KEYS = [
        # applications addon
        "AYON_APP_NAME",

        # ftrack addon
        "FTRACK_API_KEY",
        "FTRACK_API_USER",
        "FTRACK_SERVER",

        # kitsu addon
        "KITSU_SERVER",
        "KITSU_LOGIN",
        "KITSU_PWD",

        # Shotgrid / Flow addon
        "OPENPYPE_SG_USER",

        # Not sure how this is usefull for farm, scared to remove
        "PYBLISHPLUGINPATH",

        # NOTE still required by GlobalPreLoadJob.py, but might not be set by
        #   ayon-core anymore
        "AYON_DEFAULT_SETTINGS_VARIANT",
    ]

    def process(self, context):
        env = context.data.setdefault(FARM_JOB_ENV_DATA_KEY, {})
        for key in self.ENV_KEYS:
            # Skip already set keys
            if key in env:
                continue
            value = os.getenv(key)
            if value:
                self.log.debug(f"Setting job env: {key}: {value}")
                env[key] = value


class CollectAYONServerToFarmJob(CollectDeadlineJobEnvVars):
    label = "Add AYON Server URL to farm job"
    settings_category = "deadline"

    # Defined via settings
    enabled = False

    ENV_KEYS = [
        "AYON_SERVER_URL"
    ]
