{
    [... existing settings ...],
    "collect_job_info": {
        "type": "list",
        "children": [
            {
                "type": "dict",
                "keys": {
                    "job_type": {
                        "type": "string",
                        "enum": ["render", "publish", "cache"],
                        "default": "render"
                    },
                    "overrides": {
                        "type": "dict",
                        "keys": {
                            "Pool": {"type": "string", "default": ""},
                            "Priority": {"type": "int", "min": 0, "max": 100, "default": 50},
                            "ChunkSize": {"type": "int", "min": 1, "max": 100, "default": 1},
                            "ConcurrentTasks": {"type": "int", "min": 0, "max": 100, "default": 0},
                            "MachineLimit": {"type": "int", "min": 0, "max": 100, "default": 0},
                            "Group": {"type": "string", "default": ""},
                            "Department": {"type": "string", "default": ""},
                            "InitialStatus": {
                                "type": "string",
                                "enum": ["Active", "Suspended", "Deferred"],
                                "default": "Active"
                            }
                        }
                    }
                }
            }
        ]
    },
    [...]
}