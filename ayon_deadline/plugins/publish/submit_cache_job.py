import pyblish.api
from ayon_deadline.lib.deadline_profiles import DeadlineProfilesMixin


class SubmitCacheJob(pyblish.api.InstancePlugin,
                     DeadlineProfilesMixin):
    """Submit cache job to Deadline."""

    order = pyblish.api.IntegratorOrder + 0.3
    label = "Submit Cache to Deadline"
    hosts = ["maya", "houdini"]

    def process(self, instance):
        # Set job type
        instance.context.data["deadlineJobType"] = "caches"

        # Ensure deadlineJobInfo exists
        if "deadlineJobInfo" not in instance.context.data:
            instance.context.data["deadlineJobInfo"] = {}

        job_info = instance.context.data["deadlineJobInfo"]
        job_info["Plugin"] = "Python"
        job_info["Name"] = f"Cache - {instance.name}"
        job_info["BatchName"] = instance.context.data["projectName"]
        job_info["Pool"] = instance.context.data.get("caches_pool", "")
        job_info["Priority"] = instance.context.data.get("caches_priority", 50)
        # ... additional job info

        self.log.debug("Submitting cache job: %s", job_info)
