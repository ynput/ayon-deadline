import pyblish.api
from ayon_core.pipeline.publish import AYONPyblishPluginMixin


class CollectJobInfo(pyblish.api.InstancePlugin, AYONPyblishPluginMixin):
    """Collect Deadline job info overrides from profiles.

    Supports separate profiles for render, publish, and cache job types.
    """

    order = pyblish.api.CollectorOrder + 0.4
    label = "Collect Job Info"
    families = ["render", "publish", "cache"]

    def process(self, instance):
        # Determine job type from instance family
        job_type = None
        for family in instance.data.get("families", [instance.data["family"]]):
            if family in ["render", "publish", "cache"]:
                job_type = family
                break

        if not job_type:
            self.log.debug(f"No supported job type for instance: {instance}")
            return

        # Get profiles from settings
        settings = self.get_ayon_settings()
        profiles = settings.get("deadline", {}).get("publish", {}).get("CollectJobInfo", {}).get("profiles", [])

        # Collect overrides for this job type (filtered)
        overrides = {}
        for profile in profiles:
            profile_job_type = profile.get("job_type", "render")  # default to render for backward compat
            if profile_job_type != job_type:
                continue
            # Merge overrides, later profiles override earlier ones
            for key, value in profile.get("overrides", {}).items():
                overrides[key] = value

        if not overrides:
            self.log.debug(f"No overrides found for job type '{job_type}'")
            return

        # Apply overrides to instance data (prefixed with job_type to avoid conflicts)
        for key, value in overrides.items():
            prefixed_key = f"{job_type}_{key}"
            instance.data[prefixed_key] = value
            self.log.debug(f"Set override {prefixed_key} = {value}")

        # Also store original overrides under generic key for compatibility
        instance.data["deadlineJobInfoOverrides"] = overrides
        instance.data["deadlineJobType"] = job_type

    @classmethod
    def get_attribute_defs(cls):
        # Return dynamic attribute defs based on job type? Not here, but in settings.
        return []

    # Optional: provide settings schema for UI
    @classmethod
    def get_settings_schema(cls):
        return {
            "type": "object",
            "properties": {
                "profiles": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "job_type": {
                                "type": "string",
                                "enum": ["render", "publish", "cache"],
                                "default": "render"
                            },
                            "overrides": {
                                "type": "object",
                                "additionalProperties": {
                                    "type": ["string", "number", "boolean", "null"]
                                }
                            }
                        }
                    }
                }
            }
        }
