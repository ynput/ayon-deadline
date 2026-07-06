from ayon_deadline.settings import get_deadline_settings


def get_job_attributes(context, job_type):
    """Get attributes for a specific job type from settings."""
    settings = get_deadline_settings(context)
    # Assuming settings have a dict per job type
    job_settings = settings.get("job_types", {}).get(job_type, {})
    # Default attributes
    default = {
        "Priority": 50,
        "Department": "",
        "Group": "",
        "Pool": context.data.get("deadlinePool", "none"),
        "ChunkSize": 1,
        "InitialStatus": "Active",
        "OnJobComplete": "Nothing",
    }
    default.update(job_settings)
    return default


def get_job_attribute_keys(job_type):
    """Return list of attribute keys for a given job type."""
    # This could be fetched from settings schema
    from ayon_deadline.settings import DEFAULT_JOB_ATTRIBUTES
    return DEFAULT_JOB_ATTRIBUTES.get(job_type, [])
