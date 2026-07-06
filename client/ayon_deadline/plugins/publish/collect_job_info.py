import pyblish.api
from ayon_deadline import abstract_submit_deadline


class CollectJobInfoClient(abstract_submit_deadline.DeadlinePlugin):
    """Client-side counterpart for CollectJobInfo.

    Since profiles are resolved server-side, this plugin only stores
    the job type on the instance for later use by submitter plugins.
    """

    order = pyblish.api.CollectorOrder + 0.1
    label = "Collect Job Info (Client)"
    hosts = ["*"],
    families = ["*"],
    optional = True

    def process(self, instance):
        family = instance.data.get("family")
        families = instance.data.get("families", [])
        if family == "render" or "render" in families:
            job_type = "render"
        elif family == "publish" or "publish" in families:
            job_type = "publish"
        elif family == "cache" or "cache" in families:
            job_type = "cache"
        else:
            return
        instance.data["deadlineJobType"] = job_type
