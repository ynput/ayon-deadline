import os

import pyblish.api

from ayon_deadline.lib import FARM_FAMILIES, JOB_ENV_DATA_KEY
from ayon_core.pipeline import OptionalPyblishPluginMixin


class CollectAYONServerToFarmJob(pyblish.api.ContextPlugin,
                                 OptionalPyblishPluginMixin):

    order = pyblish.api.CollectorOrder
    label = "Add AYON server and API key to farm job"
    families = FARM_FAMILIES
    targets = ["local"]

    settings_category = "deadline"

    # Defined via settings
    enabled = False
    ayon_api_key: str = ""

    def process(self, context):
        if not self.is_active(context.data):
            return

        # The AYON API key will be set from settings if any value is provided.
        # Otherwise, we allow it to fall back to the current API key. However,
        # it is recommended to specify a service user API key in settings
        # instead of passing the user's API key to avoid issues when e.g.
        # deactivating a user account.
        ayon_api_key = self.ayon_api_key
        if not ayon_api_key:
            self.log.debug("No `AYON_API_KEY` specified in settings. "
                           "Falling back to current session's API key.")
            ayon_api_key = os.getenv("AYON_API_KEY")

        env = {
            "AYON_SERVER_URL": os.getenv("AYON_SERVER_URL"),
            "AYON_API_KEY": ayon_api_key
        }
        for key, value in env.items():
            self.log.debug(f"Setting job env: {key}: {value}")

        # Transfer some environment variables from current context
        context.data.setdefault(JOB_ENV_DATA_KEY, {}).update(env)
