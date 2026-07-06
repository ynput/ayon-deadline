from ayon_core.pipeline.migration import BaseMigration

class MigrateJobTypeToProfiles(BaseMigration):
    """Add job_type field to existing Deadline profiles and migrate old attributes."""

    version = 2
    description = "Add job_type field to Deadline profiles. Old profiles without 'job_type' are assumed to be render."

    def migrate(self, data):
        profiles_list = data.get("profiles", [])
        for profile in profiles_list:
            if "job_type" not in profile:
                profile["job_type"] = "render"
        return data
