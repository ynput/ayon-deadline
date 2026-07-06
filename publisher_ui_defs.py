from ayon_core.pipeline.create import Creator
from ayon_deadline import lib


class DeadlineJobInfoCollector(object):
    """Add job-type-specific UI definitions."""

    def __init__(self, creator: Creator):
        self.creator = creator

    def get_attribute_defs(self):
        # This would return appropriate UI defs based on selected job type
        # In reality, this would be integrated into the Creator
        pass

# Example: Register multiple defs with prefix
from ayon_core.pipeline import KnownPublishTypes
from ayon_core.pipeline.create import Creator

def create_job_info_defs(creator):
    job_type = creator.data.get("jobType", "render")
    settings = lib.get_deadline_settings()
    profiles = settings.get("job_info_profiles", [])
    
    defs = []
    for profile in profiles:
        if profile.get("job_type") == job_type:
            attr_def = profile.get("attribute_def", {})
            # Build UI defs from attribute definition
            # This would need to map to standard Creator UI defs
            pass
    return defs
