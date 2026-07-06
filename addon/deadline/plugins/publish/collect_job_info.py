import pyblish.api
from ayon_core.pipeline import AYONPyblishPluginMixin
from ayon_deadline import api


class CollectJobInfo(pyblish.api.ContextPlugin):
    order = pyblish.api.CollectorOrder - 0.49
    label = "Collect Job Info"
    hosts = ["*"](*

    def process(self, context):
        # Identify job type from context data
        job_type = context.data.get("jobType", "render")
        if job_type not in ["render", "publish", "cache"]:
            job_type = "render"

        # Load profile for job type
        profiles = api.get_deadline_profiles(context)
        for profile in profiles:
            if profile.get("job_type") == job_type or profile.get("job_type") == "*":
                # Apply settings from profile
                self._apply_profile(context, profile, job_type)
                break

    def _apply_profile(self, context, profile, job_type):
        # Apply common settings
        for key in ["priority", "department", "pool", "group"]:
            value = profile.get(f"{job_type}_{key}", profile.get(key))
            if value is not None:
                context.data[f"deadlineJobInfo.{key}"] = value

        # Additional fields per job type
        if job_type == "render":
            for key in ["frames_per_task", "concurrent_tasks"]:
                value = profile.get("render_" + key, profile.get(key))
                if value is not None:
                    context.data[f"deadlineJobInfo.{key}"] = value
        elif job_type == "publish":
            pass  # Publish-specific fields
        elif job_type == "cache":
            pass  # Cache-specific fields
