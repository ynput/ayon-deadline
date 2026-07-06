# Existing publish job collector, modified to set job_type
import pyblish.api

class CollectPublishJob(pyblish.api.Collector):
    order = pyblish.api.CollectorOrder - 0.48
    label = "Collect Publish Job"
    hosts = ["*"](*

    def process(self, context):
        # Set job type for later use
        context.data["jobType"] = "publish"
        # Original logic continues...
        # Remove any direct setting of DeadlineJobInfo keys that should come from CollectJobInfo
