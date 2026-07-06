import pyblish.api
from ayon_core.pipeline import AYONPyblishPluginMixin
from ayon_core.lib import BoolDef, EnumDef, NumberDef, TextDef

class CollectJobInfo(pyblish.api.InstancePlugin, AYONPyblishPluginMixin):
    """Collect job info from profiles and store on instance."""

    order = pyblish.api.CollectorOrder + 0.45
    label = "Collect Job Info"

    def process(self, instance):
        # Get existing job type from instance (set by previous plugin)
        job_type = instance.data.get("jobType", "render")
        
        # Retrieve profile matching current context
        profile = self._get_matching_profile(instance, job_type)
        if profile is None:
            self.log.debug("No matching job info profile, skipping.")
            return

        # Store profile data on instance for later use
        instance.data["deadlineJobInfo"] = {
            "JobType": job_type,
            "Priority": profile.get("priority", 50),
            # Include other profile fields based on job_type?
            # For now, we store all fields; later submission plugin will filter.
            "_profile": profile
        }

    def _get_matching_profile(self, instance, job_type):
        profiles = self._get_collection_settings()["profiles"]
        for profile in profiles:
            # Check host, family, etc. (existing logic) AND job_type
            if self._match_instance(instance, profile) and \
               profile.get("job_type", "render") == job_type:
                return profile
        return None
