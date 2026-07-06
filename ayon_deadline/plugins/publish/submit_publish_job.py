import pyblish.api
from ayon_core.pipeline.publish import KnownPublishError
from ayon_core.lib import BoolDef, NumberDef, TextDef
from ayon_deadline.lib.deadline_profiles import DeadlineProfilesMixin


class SubmitPublishJob(pyblish.api.InstancePlugin,
                       DeadlineProfilesMixin):
    """Submit publish job to Deadline."""

    order = pyblish.api.IntegratorOrder + 0.3
    label = "Submit Publish to Deadline"
    hosts = ["maya", "nuke", "houdini"]

    def process(self, instance):
        # Set job type for this instance
        instance.context.data["deadlineJobType"] = "publish"

        # Collect job info (will be done by CollectDeadlineJobInfo)
        # Ensure deadlineJobInfo exists
        if "deadlineJobInfo" not in instance.context.data:
            instance.context.data["deadlineJobInfo"] = {}

        # Build job info specific to publish
        job_info = instance.context.data["deadlineJobInfo"]
        job_info["Plugin"] = "Python"
        job_info["Name"] = f"Publish - {instance.name}"
        job_info["BatchName"] = instance.context.data["projectName"]
        job_info["Pool"] = instance.context.data.get("publish_pool", "")
        job_info["Priority"] = instance.context.data.get("publish_priority", 50)
        # ... additional job info

        # Create job submission
        self.log.debug("Submitting publish job: %s", job_info)
