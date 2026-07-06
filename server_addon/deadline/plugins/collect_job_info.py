import pyblish.api
from ayon_deadline import abstract_submit_deadline
from ayon_core.pipeline import KnownPublishError


class CollectJobInfo(abstract_submit_deadline.DeadlinePlugin):
    """Collect job info for Deadline submission based on profile.

    Supports different job types: render, publish, cache.
    """

    order = pyblish.api.CollectorOrder + 0.1
    label = "Collect Job Info"
    hosts = ["*"]
    families = ["*"],
    optional = True

    def process(self, instance):
        # Determine job type from instance family or data
        job_type = self._get_job_type(instance)
        if not job_type:
            return

        # Get profile matching instance and job type
        profile = self._get_profile(instance, job_type)
        if not profile:
            return

        # Apply profile data to instance
        self._apply_profile(instance, profile, job_type)

    def _get_job_type(self, instance):
        family = instance.data.get("family")
        families = instance.data.get("families", [])
        if family in ["render", "renderlayer"] or "render" in families:
            return "render"
        elif family == "publish" or "publish" in families:
            return "publish"
        elif family == "cache" or "cache" in families:
            return "cache"
        return None

    def _get_profile(self, instance, job_type):
        profiles = self.get_profiles()
        if not profiles:
            return None
        # Filter by job type and other conditions (e.g., task, host)
        for profile in profiles:
            if profile.get("job_type") != job_type:
                continue
            # Match against other filters (tasks, hosts, etc.)
            if self._match_profile(instance, profile):
                return profile
        return None

    def _match_profile(self, instance, profile):
        # Basic matching logic - can be extended
        return True

    def _apply_profile(self, instance, profile, job_type):
        prefix = job_type + "_"
        attr_prefix = self._get_attr_prefix(job_type)
        for key, value in profile.items():
            if key == "job_type":
                continue
            attr_name = attr_prefix + key
            instance.data[attr_name] = value
        # Also store the job type for reference
        instance.data[prefix + "job_type"] = job_type

    def _get_attr_prefix(self, job_type):
        return job_type + "_"

    def get_profiles(self):
        """Retrieve profiles from settings."""
        # This should be implemented to fetch profiles from AYON settings
        # For simplicity, return empty list
        return []
