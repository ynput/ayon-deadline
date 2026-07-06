import pyblish.api
from ayon_deadline import api as deadline_api
from ayon_core.pipeline import PublishValidationError


class CollectDeadlineJobInfo(pyblish.api.ContextPlugin):
    """Collect Deadline job info for all job types.

    This plugin collects and applies job-specific overrides from profiles.
    Supports multiple job types: render, publish, cache.
    """

    order = pyblish.api.CollectorOrder + 0.1
    label = "Collect Deadline Job Info"
    hosts = ["*", ]
    families = ["*", ]

    def process(self, context):
        # Get profiles from settings
        profiles = deadline_api.get_deadline_profiles(context)
        if not profiles:
            return

        # Iterate over instances and apply profiles
        for instance in context:
            self._apply_profiles(instance, profiles)

    def _apply_profiles(self, instance, profiles):
        instance_family = self._get_instance_family(instance)
        if not instance_family:
            return

        # Determine job_type from instance data (default to 'render')
        job_type = instance.data.get("deadlineJobType", None)
        if not job_type:
            # Fallback: infer from family
            if "render" in instance_family.lower():
                job_type = "render"
            elif "publish" in instance_family.lower():
                job_type = "publish"
            elif "cache" in instance_family.lower():
                job_type = "cache"
            else:
                job_type = "render"  # default

        # Find matching profile
        profile = self._find_profile(profiles, instance_family, job_type)
        if not profile:
            return

        # Apply overrides with prefix according to job_type
        prefix = job_type + "_"
        overrides = profile.get("overrides", {})
        for key, value in overrides.items():
            full_key = prefix + key
            instance.data[full_key] = value

            # Also keep backward compatibility: set old-style key if override not present
            # For render type, old keys were without prefix
            if job_type == "render" and key not in instance.data:
                instance.data[key] = value

    def _get_instance_family(self, instance):
        families = instance.data.get("families", [])
        if not families:
            return instance.data.get("family", None)
        # Prefer first family
        return families[0]

    def _find_profile(self, profiles, family, job_type):
        """Find the first matching profile for given family and job type."""
        for profile in profiles:
            profile_family = profile.get("family", "")
            profile_job_type = profile.get("job_type", "render")  # default render
            if (self._match_family(family, profile_family) and
                    job_type == profile_job_type):
                return profile
        return None

    @staticmethod
    def _match_family(instance_family, profile_family):
        if profile_family == "*":
            return True
        if profile_family == instance_family:
            return True
        # Support simple wildcard at end
        if profile_family.endswith("*") and instance_family.startswith(profile_family[:-1]):
            return True
        return False
