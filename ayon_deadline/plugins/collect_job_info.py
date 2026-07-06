import pyblish.api
from ayon_deadline import api


class CollectJobInfo(pyblish.api.ContextPlugin):
    """Collect Deadline job info from profiles.

    This plugin now supports all job types (render, publish, cache).
    """

    order = pyblish.api.CollectorOrder + 0.499
    label = "Collect Job Info"
    hosts = ["fusion", "maya", "nuke", "houdini", "blender", "aftereffects",
             "celaction", "harmony", "photoshop", "resolve"]

    def process(self, context):
        # Get job type from context data, default to 'render' for backward compat
        job_type = context.data.get("jobType", "render")

        # Get the project settings
        project_settings = context.data["project_settings"]
        deadline_settings = project_settings.get("deadline", {})

        # Get profiles
        profiles = deadline_settings.get("job_info_profiles", [])

        # Find matching profile for this job type
        profile = self._find_matching_profile(profiles, job_type, context)

        if not profile:
            return

        job_info = profile.get("job_info", {})
        if not job_info:
            return

        # Merge job info into context.data['deadlineJobInfo']
        deadline_job_info = context.data.setdefault("deadlineJobInfo", {})
        for key, value in job_info.items():
            if key == "job_type":
                continue
            # Skip empty values
            if value is None or (isinstance(value, str) and not value.strip()):
                continue
            deadline_job_info[key] = value

        # Also store job type specific overrides
        context.data["deadlineJobInfo_" + job_type] = deadline_job_info

    def _find_matching_profile(self, profiles, job_type, context):
        """Find the first profile that matches the job type and other filters."""
        for profile in profiles:
            profile_job_type = profile.get("job_type", "render")
            if profile_job_type != job_type:
                continue
            # Additional matching logic can be added here
            # e.g., task type, host, etc.
            return profile
        return None
