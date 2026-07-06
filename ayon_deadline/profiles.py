import copy


def get_default_profile():
    return {
        "job_type": "render",  # default for backward compatibility
        "priority": 50,
        "chunk_size": 1,
        "pool": "none",
        "group": "none",
        "department": "",
        "machine_list": "",
        "limit_groups": "",
        "concurrent_tasks": 1,
        "enforce_limit_groups": False,
        "on_complete": "none",
        "suspended": False,
        "whitelist": "",
        "blacklist": "",
        "initial_status": "Active",
    }


def get_default_job_type_profiles():
    return {
        "render": {
            "priority": 50,
            "chunk_size": 1,
            "pool": "none",
            "group": "none",
            "department": "",
            "machine_list": "",
            "limit_groups": "",
            "concurrent_tasks": 1,
            "enforce_limit_groups": False,
            "on_complete": "none",
            "suspended": False,
            "whitelist": "",
            "blacklist": "",
            "initial_status": "Active",
        },
        "publish": {
            "priority": 50,
            "chunk_size": 1,
            "pool": "none",
            "group": "none",
            "department": "",
            "machine_list": "",
            "limit_groups": "",
            "concurrent_tasks": 1,
            "enforce_limit_groups": False,
            "on_complete": "none",
            "suspended": False,
            "whitelist": "",
            "blacklist": "",
            "initial_status": "Active",
        },
        "cache": {
            "priority": 50,
            "chunk_size": 1,
            "pool": "none",
            "group": "none",
            "department": "",
            "machine_list": "",
            "limit_groups": "",
            "concurrent_tasks": 1,
            "enforce_limit_groups": False,
            "on_complete": "none",
            "suspended": False,
            "whitelist": "",
            "blacklist": "",
            "initial_status": "Active",
        },
    }


def migrate_old_profile(profile: dict) -> dict:
    """Convert old profile (without job_type) to new format."""
    if "job_type" in profile:
        return profile
    # Assume old profile was for render
    new_profile = copy.deepcopy(profile)
    new_profile["job_type"] = "render"
    # Remove any fields that are job-type specific (they are already there but we keep)
    return new_profile
