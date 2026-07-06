import pyblish.api
from ayon_core.pipeline import KnownPublishTypes
from ayon_deadline import lib


class SubmitPublishJob(pyblish.api.InstancePlugin):
    """Submit publish job to Deadline."""

    order = pyblish.api.IntegratorOrder + 0.1
    label = "Submit Publish Job"
    families = ["render", "publish", "caches"]

    def process(self, instance):
        # Get job type from instance
        job_type = None
        for family in instance.data.get("families", []):
            if family in [KnownPublishTypes.RENDER, KnownPublishTypes.PUBLISH, KnownPublishTypes.CACHES]:
                job_type = family
                break
        if not job_type:
            job_type = KnownPublishTypes.RENDER

        # Extract attributes stored with prefix
        context = instance.context
        attributes = {}
        prefix = f"deadline_{job_type}_"
        for key, value in context.data.items():
            if key.startswith(prefix):
                attr_name = key[len(prefix):]
                attributes[attr_name] = value

        # Build job info for Deadline
        job_info = {
            "JobType": "Normal",
            "Name": instance.data["name"],
            "Plugin": instance.data.get("deadlinePlugin", ".."),
            "Priority": attributes.get("Priority", 50),
            "Department": attributes.get("Department", ""),
            "Group": attributes.get("Group", ""),
            "Pool": attributes.get("Pool", "none"),
            "ChunkSize": attributes.get("ChunkSize", 1),
            "InitialStatus": attributes.get("InitialStatus", "Active"),
            "OnJobComplete": attributes.get("OnJobComplete", "Nothing"),
        }

        # Store job_info in instance data for later submission
        instance.data["deadlineJobInfo"] = job_info
