import pyblish.api
from ayon_core.pipeline import KnownPublishError
from ayon_deadline import lib


class CollectJobInfo(pyblish.api.ContextPlugin):
    """Collect job information for Deadline submission based on profiles."""

    order = pyblish.api.CollectorOrder + 0.2
    label = "Collect Job Info"

    def process(self, context):
        project_name = context.data["projectName"]
        folder_path = context.data["folderPath"]
        task_name = context.data.get("task")

        # Get profiles from settings
        profiles = self._get_profiles(project_name)
        if not profiles:
            self.log.debug("No job info profiles found")
            return

        # Collect all job types from instances
        job_types = set()
        for instance in context:
            family = instance.data.get("family")
            job_type = lib.get_job_type_for_family(family)
            if job_type:
                job_types.add(job_type)

        if not job_types:
            self.log.debug("No job types determined from instances")
            return

        # Apply profiles for each job type
        for job_type in job_types:
            profile = self._find_matching_profile(profiles, job_type)
            if not profile:
                self.log.debug("No matching profile for job type: %s", job_type)
                continue

            overrides = self._get_overrides(profile, job_type)
            if not overrides:
                continue

            # Store overrides in context data with job type prefix
            override_key = f"deadlineJobInfo_{job_type}"
            existing = context.data.get(override_key, {})
            existing.update(overrides)
            context.data[override_key] = existing

            self.log.info("Collected job info overrides for %s: %s", job_type, overrides)

    def _get_profiles(self, project_name):
        """Get job info profiles from settings.

        Override in subclass or use settings manager.
        """
        return lib.get_project_setting(
            project_name, "deadline", "publish", "CollectJobInfo", "profiles"
        )

    def _find_matching_profile(self, profiles, job_type):
        """Find the first profile that matches the job type."""
        for profile in profiles:
            profile_type = profile.get("job_type")
            if profile_type == job_type or profile_type == "all":
                return profile
        return None

    def _get_overrides(self, profile, job_type):
        """Extract safe overrides from profile based on job type.

        Skips fields that are not relevant for the job type.
        """
        available_fields = lib.get_job_type_fields(job_type)
        overrides = {}
        for key, value in profile.items():
            if key == "job_type":
                continue
            # Convert profile key to Deadline attribute name (e.g., "priority" -> "Priority")
            attr_name = key.capitalize()
            if attr_name in available_fields:
                overrides[attr_name] = value
        return overrides


class CollectRenderJobInfo(CollectJobInfo):
    """Legacy collector for render jobs (backward compatibility)."""

    def process(self, context):
        # First check if new-style overrides already exist
        if "deadlineJobInfo_render" in context.data:
            return

        # Fallback: collect from old-style profiles without job type
        project_name = context.data["projectName"]
        profiles = self._get_profiles(project_name)
        if not profiles:
            return

        old_profile = self._find_matching_profile(profiles, "render")
        if old_profile:
            overrides = self._get_overrides(old_profile, "render")
            if overrides:
                context.data["deadlineJobInfo_render"] = overrides
