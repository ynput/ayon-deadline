import pyblish.api
from ayon_core.pipeline import KnownPublishTypes
from ayon_deadline import lib


class CollectJobInfo(pyblish.api.ContextPlugin):
    """Collect job info for Deadline submission."""

    order = pyblish.api.CollectorOrder + 0.2
    label = "Collect Deadline Job Info"
    hosts = ["*"]

    def process(self, context):
        # Determine job type from context
        job_type = self._get_job_type(context)
        attributes = lib.get_job_attributes(context, job_type)
        # Store attributes with prefix to avoid conflicts
        for key, value in attributes.items():
            context.data[f"deadline_{job_type}_{key}"] = value

    def _get_job_type(self, context):
        known_types = [
            KnownPublishTypes.RENDER,
            KnownPublishTypes.PUBLISH,
            KnownPublishTypes.CACHES
        ]
        for t in known_types:
            if t in context.data.get("publishTypes", []):
                return t
        return KnownPublishTypes.RENDER  # default to render
