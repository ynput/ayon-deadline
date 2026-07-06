import pyblish.api
from ayon_core.pipeline import KnownPublishTypes
from ayon_deadline import lib
from ayon_deadline.abstract import DeadlineJobInfo


class CollectDeadlineJobInfo(pyblish.api.ContextPlugin):
    """Collect Deadline job info from profiles."""

    order = pyblish.api.CollectorOrder + 0.4
    label = "Collect Deadline Job Info (All Jobs)"

    def process(self, context):
        # Get job type from context (render, publish, cache)
        # Assume instance.data.get("jobType", "render") or from family
        job_type = context.data.get("jobType", "render")
        
        # Get profiles from settings
        self._collect_job_info_profiles(context, job_type)

    def _collect_job_info_profiles(self, context, job_type):
        # Profile matching logic (simplified)
        settings = lib.get_deadline_settings()
        profiles = settings.get("job_info_profiles", [])
        
        for profile in profiles:
            # Check if profile matches current context
            profile_job_type = profile.get("job_type", "render")
            if profile_job_type != "*" and profile_job_type != job_type:
                continue
            # Other matching criteria (tasks, families, etc.)
            if not self._match_profile(profile, context):
                continue
            # Apply attributes
            self._apply_attributes(context, profile)
            break

    def _match_profile(self, profile, context):
        # Placeholder for actual matching logic
        return True

    def _apply_attributes(self, context, profile):
        # Apply attributes with job type prefix to avoid conflicts
        job_type = context.data.get("jobType", "render")
        prefix = job_type + "_"
        attributes = profile.get("attributes", {})
        for key, value in attributes.items():
            qualified_key = prefix + key
            context.data[qualified_key] = value
