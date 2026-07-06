import pyblish.api
from ayon_core.pipeline import KnownPublishTypes
from ayon_deadline import lib


class SubmitCacheJob(pyblish.api.InstancePlugin):
    """Submit cache job to Deadline."""

    order = pyblish.api.IntegratorOrder + 0.1
    label = "Submit Cache Job"
    families = ["caches"]

    def process(self, instance):
        job_type = KnownPublishTypes.CACHES
        context = instance.context
        attributes = {}
        prefix = f"deadline_{job_type}_"
        for key, value in context.data.items():
            if key.startswith(prefix):
                attr_name = key[len(prefix):]
                attributes[attr_name] = value

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
        instance.data["deadlineJobInfo"] = job_info
