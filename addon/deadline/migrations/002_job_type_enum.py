from ayon_core.lib import Migration


class Migration002JobTypeEnum(Migration):
    """Add job_type enum to existing profiles."""

    def apply(self, settings_data):
        # Ensure every profile has a job_type field, default to "render"
        for profile in settings_data.get("deadline", {}).get("profiles", []):
            if "job_type" not in profile:
                profile["job_type"] = "render"
        return settings_data

    def rollback(self, settings_data):
        # Remove job_type field (optional)
        for profile in settings_data.get("deadline", {}).get("profiles", []):
            profile.pop("job_type", None)
        return settings_data
