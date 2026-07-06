from ayon_core.pipeline import KnownPublishError


_JOB_TYPE_FIELDS = {
    "render": {"Priority", "ChunkSize", "ConcurrentTasks", "MachineLimit",
                "ForceReload", "InitialStatus", "Pool", "SecondaryPool",
                "Group", "Department", "OnJobComplete", "OutputFilename0",
                "OutputDirectory0"},
    "publish": {"Priority", "Pool", "SecondaryPool", "Group",
                 "Department", "OnJobComplete"},
    "cache": {"Priority", "Pool", "SecondaryPool", "Group",
               "Department", "OnJobComplete", "ChunkSize", "ConcurrentTasks"},
}


# Mapping from family to job type (should be configurable, here simplified)
_FAMILY_JOB_TYPE_MAP = {
    "render": "render",
    "render.farm": "render",
    "publish.farm": "publish",
    "cache.farm": "cache",
}


def get_job_type_for_family(family):
    """Return job type string for a given family."""
    return _FAMILY_JOB_TYPE_MAP.get(family)


def get_job_type_fields(job_type):
    """Return set of valid field names for a job type."""
    return _JOB_TYPE_FIELDS.get(job_type, set())


def get_project_setting(project_name, *keys):
    """Dummy implementation to fetch project settings.

    In production, use ayon_core's settings API.
    """
    # Placeholder: return empty list/dict
    return []
