from ayon_core.addon import AddonMigration

class AddJobTypeToProfiles(AddonMigration):
    """Add job_type field to existing profiles, defaulting to 'render'."""

    def run(self):
        # Load existing settings
        settings = self.get_settings()
        profiles = settings.get("collect_job_info", {}).get("profiles", [])
        modified = False
        for profile in profiles:
            if "job_type" not in profile:
                profile["job_type"] = "render"
                modified = True
        if modified:
            self.set_settings(settings)
            self.log.info("Migrated profiles: added job_type field.")
