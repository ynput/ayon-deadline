from ayon_core.lib import EnumDef

def get_collect_job_info_profile():
    """Return profile schema for CollectJobInfo."""
    return {
        "type": "dict",
        "keys": {
            "job_type": {
                "type": "string",
                "enum": [
                    {"value": "render", "label": "Render"},
                    {"value": "publish", "label": "Publish"},
                    {"value": "cache", "label": "Cache"}
                ],
                "default": "render"
            },
            "overrides": {
                "type": "dict",
                "keys": {
                    "Pool": {"type": "string", "default": ""},
                    "Priority": {
                        "type": "int",
                        "min": 0,
                        "max": 100,
                        "default": 50
                    },
                    "ChunkSize": {
                        "type": "int",
                        "min": 1,
                        "max": 100,
                        "default": 1
                    },
                    "ConcurrentTasks": {
                        "type": "int",
                        "min": 0,
                        "max": 100,
                        "default": 0
                    },
                    "MachineLimit": {
                        "type": "int",
                        "min": 0,
                        "max": 100,
                        "default": 0
                    },
                    "Group": {"type": "string", "default": ""},
                    "Department": {"type": "string", "default": ""},
                    "InitialStatus": {
                        "type": "string",
                        "enum": [
                            "Active",
                            "Suspended",
                            "Deferred"
                        ],
                        "default": "Active"
                    }
                }
            }
        }
    }