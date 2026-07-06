import copy
from ayon_core.settings import get_project_settings
from ayon_core.settings.schema import (
    create_schema_node,
    create_list_node,
    create_dict_node,
    create_enum_node,
    create_number_node,
    create_string_node,
    create_boolean_node
)

# Default visible attributes per job type
DEFAULT_JOB_ATTRIBUTES = {
    "render": ["Priority", "Department", "Group", "Pool", "ChunkSize", "InitialStatus", "OnJobComplete"],
    "publish": ["Priority", "Department", "Group", "InitialStatus"],
    "caches": ["Priority", "Department", "Group", "Pool", "InitialStatus"]
}

def get_deadline_settings(context):
    project = context.data.get("projectName")
    if not project:
        return {}
    settings = get_project_settings(project)
    return settings.get("deadline", {})

def create_job_type_def(job_type):
    """Create a settings definition for a specific job type."""
    # Example schema: each attribute as a node
    node = create_dict_node(
        label=job_type.capitalize(),
        children=[
            create_number_node("Priority", default=50),
            create_string_node("Department"),
            create_string_node("Group"),
            create_string_node("Pool", default="none"),
            create_number_node("ChunkSize", default=1),
            create_enum_node("InitialStatus", items=["Active", "Suspended"]),
            create_enum_node("OnJobComplete", items=["Nothing", "Delete", "Archive"]),
        ]
    )
    return node

def get_settings_schema():
    # This would be part of the Deadline addon's settings schema
    # For now, return a placeholder
    return create_dict_node(
        label="Deadline",
        children=[
            create_job_type_def("render"),
            create_job_type_def("publish"),
            create_job_type_def("caches"),
        ]
    )
