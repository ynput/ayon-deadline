import pyblish.api

from ayon_core.pipeline import (
    PublishValidationError,
    OptionalPyblishPluginMixin
)
from ayon_deadline.lib import FARM_FAMILIES


class ValidateDeadlineJobInfo(
    OptionalPyblishPluginMixin,
    pyblish.api.InstancePlugin
):
    """Validate collected values for JobInfo section in Deadline submission

    """

    label = "Validate Deadline JobInfo"
    order = pyblish.api.ValidatorOrder
    families = FARM_FAMILIES
    optional = True
    targets = ["local"]

    # cache
    pools_by_url = {}

    def process(self, instance):
        if not self.is_active(instance.data):
            return

        if not instance.data.get("farm"):
            self.log.debug("Skipping local instance.")
            return

        priority = instance.data["deadline"]["job_info"].Priority
        if priority < 0 or priority > 100:
            raise PublishValidationError(
                f"Priority:'{priority}' must be between 0-100")
