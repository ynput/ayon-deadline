import pyblish.api
from ayon_core.pipeline import publish
from ayon_deadline.lib import DeadlineExporter


class CollectJobInfo(publish.Collector):
    """Collect job info for Deadline based on profile and job type."""

    order = pyblish.api.CollectorOrder - 0.5
    label = "Collect Job Info"

    def process(self, instance):
        # Get job type from instance.data (set by previous plugins)
        job_type = instance.data.get("deadlineJobType", "render")

        # Get profile settings from project settings
        settings = instance.context.data["project_settings"]["deadline"]["CollectJobInfo"]
        profiles = settings.get("profiles", [])

        # Find matching profile based on task type, host, etc.
        matching_profile = None
        for profile in profiles:
            if profile.get("job_type") != "*" and profile["job_type"] != job_type:
                continue
            # Additional matching logic (host, task, etc.) can be added
            # For simplicity, we pick the first applicable
            if matching_profile is None:
                matching_profile = profile
                break

        if not matching_profile:
            return

        # Apply settings from profile with appropriate key prefix
        prefix = f"{job_type}_" if job_type != "render" else ""
        for key, value in matching_profile.items():
            if key == "job_type":
                continue
            prefixed_key = f"{prefix}{key}"
            # Store in instance data for later use
            instance.data[f"deadline{prefixed_key}"] = value

        # Add job type for reference
        instance.data["deadlineJobType"] = job_type


# Migration: rename old keys to new prefixed ones
def migrate_workfile_data(workfile_data):
    """Update old workfile data to new prefixed keys."""
    if not workfile_data:
        return
    # In real implementation, this would loop through stored overrides
    # and rename keys like "Priority" to "render_Priority"
    pass
