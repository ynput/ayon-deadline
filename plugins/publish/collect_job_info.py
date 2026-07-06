import json
from ayon_core.pipeline import get_current_project
from ayon_core.pipeline.publish import (
    AYONPyblishPluginMixin,
    PublishValidationError,
)
from ayon_deadline import lib as deadline_lib
from ayon_deadline.abstract_submit_deadline import AbstractSubmitDeadline


class CollectJobInfo(AYONPyblishPluginMixin):
    """Collect Deadline job info overrides from profiles.

    This plugin collects job info overrides based on the job type
    (render, publish, cache) and applies them to the instance.
    """

    order = 100
    label = "Collect Job Info Overrides"
    hosts = ["*", "!standalone"]
    families = ["render", "publish", "cache"]

    def process(self, instance):
        project = get_current_project()
        profiles = project.get_ayon_settings()[
            "deadline"]
        profiles = profiles.get("collect_job_info", [])

        # Determine job type from instance family/attribute
        family = instance.data.get("family")
        if family in ("render", "publish", "cache"):
            job_type = family
        else:
            self.log.debug(f"Skipping instance with family '{family}'")
            return

        # Find matching profile
        matching_profile = None
        for profile in profiles:
            if profile["job_type"] == job_type:
                matching_profile = profile
                break

        if not matching_profile:
            self.log.debug(f"No profile found for job type '{job_type}'")
            return

        overrides = matching_profile.get("overrides", {})
        if not overrides:
            return

        # Apply overrides to instance
        # Each override field will be stored with job_type prefix in instance data
        # to avoid conflicts between different job types.
        # Existing non-prefixed overrides (from old workfiles) will be migrated.
        prefix = f"{job_type}_"
        for key, value in overrides.items():
            if value is not None and value != "" and value != 0:
                attr_name = prefix + key
                instance.data[attr_name] = value
                self.log.debug(f"Applied override: {attr_name} = {value}")

        # Also set the original attribute (for compatibility with other plugins)
        # but only if it's not already set
        for key, value in overrides.items():
            if value is not None and value != "" and value != 0:
                if key not in instance.data or instance.data[key] is None:
                    instance.data[key] = value
                    self.log.debug(f"Applied non-prefixed override: {key} = {value}")

        # Migrate old unset overrides if any (would be handled on first run)
        # This is a placeholder for actual migration logic.
        self._migrate_old_overrides(instance, job_type)

    def _migrate_old_overrides(self, instance, job_type):
        """Migrate old workfiles without job_type prefix.

        Old workfiles stored overrides as e.g. 'Priority' without prefix.
        We need to convert them to 'render_Priority' if they belong to render.
        """
        # For now just log; full implementation would read old attributes
        # and store them with prefix, then clear old ones.
        self.log.debug("Migration of old overrides not yet fully implemented.")


# Register plugin
from ayon_core.pipeline.publish import register_plugin_path
register_plugin_path(__file__)