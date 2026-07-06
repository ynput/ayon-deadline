import pyblish.api

class CollectCacheJob(pyblish.api.Collector):
    order = pyblish.api.CollectorOrder - 0.48
    label = "Collect Cache Job"
    hosts = ["*"](*

    def process(self, context):
        context.data["jobType"] = "cache"
        # Additional cache-specific collection logic
