import pyblish.api

from ayon_core.pipeline import (
    PublishValidationError,
    OptionalPyblishPluginMixin
)
from ayon_core.pipeline.farm.pyblish_functions import \
    convert_frames_str_to_list
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

        custom_frames = instance.data["deadline"]["job_info"].Frames
        if not custom_frames:
            return

        frame_start = (
            instance.data.get("frameStart")
            or instance.context.data.get("frameStart")
        )
        frame_end = (
            instance.data.get("frameEnd")
            or instance.context.data.get("frameEnd")
        )
        if not frame_start or not frame_end:
            self.log.info("Unable to get frame range, skip validation.")
            return

        frames_to_render = convert_frames_str_to_list(custom_frames)

        if (
            min(frames_to_render) < frame_start
            or max(frames_to_render) > frame_end
        ):
            raise PublishValidationError(
                f"Custom frames '{custom_frames}' are outside of "
                f"expected frame range '{frame_start}'-'{frame_end}'"
            )
