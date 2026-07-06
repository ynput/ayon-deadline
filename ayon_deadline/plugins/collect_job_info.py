from ayon_deadline import profiles
from ayon_core.pipeline import PLUGIN_ACTION_CONTEXT
from ayon_core.pipeline.publish import (
    ValidatePipelineOrder,
    PublishValidationError,
    KnownPublishError,
)
from ayon_core.pipeline.publish import (
    get_plugin_settings_from_profile,
    get_subset_name_from_profile,
)
from ayon_core.pipeline.publish.collectors import BaseCollector
from ayon_core.pipeline.publish.abstract_collector import AbstractCollector
from ayon_core.pipeline.publish.lib import get_instances_for_context


class CollectJobInfo(BaseCollector):
    """Collect job info for Deadline based on profile."""

    order = ValidatePipelineOrder
    label = "Collect Job Info"
    hosts = ["*"]
    families = ["render", "publish", "cache"]

    settings_category = "deadline"

    @classmethod
    def get_attribute_defs(cls):
        from ayon_core.lib.attribute_definitions import (
            NumberDef,
            TextDef,
            BoolDef,
            EnumDef,
            SeparatorDef,
        )

        defs = []
        for job_type in ["render", "publish", "cache"]:
            prefix = f"{job_type}_"
            defs.append(SeparatorDef(label=f"{job_type.capitalize()} Job Settings"))
            defs.append(
                NumberDef(
                    f"{prefix}priority",
                    default=50,
                    minimum=0,
                    maximum=100,
                    label="Priority",
                )
            )
            defs.append(
                NumberDef(
                    f"{prefix}chunk_size",
                    default=1,
                    minimum=0,
                    maximum=1000,
                    label="Chunk Size",
                )
            )
            defs.append(
                TextDef(
                    f"{prefix}pool",
                    default="none",
                    label="Pool",
                )
            )
            defs.append(
                TextDef(
                    f"{prefix}group",
                    default="none",
                    label="Group",
                )
            )
            defs.append(
                TextDef(
                    f"{prefix}department",
                    default="",
                    label="Department",
                )
            )
            defs.append(
                TextDef(
                    f"{prefix}machine_list",
                    default="",
                    label="Machine List",
                )
            )
            defs.append(
                TextDef(
                    f"{prefix}limit_groups",
                    default="",
                    label="Limit Groups",
                )
            )
            defs.append(
                NumberDef(
                    f"{prefix}concurrent_tasks",
                    default=1,
                    minimum=0,
                    maximum=1000,
                    label="Concurrent Tasks",
                )
            )
            defs.append(
                BoolDef(
                    f"{prefix}enforce_limit_groups",
                    default=False,
                    label="Enforce Limit Groups",
                )
            )
            defs.append(
                EnumDef(
                    f"{prefix}on_complete",
                    items=["none", "delete", "archive"],
                    default="none",
                    label="On Complete",
                )
            )
            defs.append(
                BoolDef(
                    f"{prefix}suspended",
                    default=False,
                    label="Suspended",
                )
            )
            defs.append(
                TextDef(
                    f"{prefix}whitelist",
                    default="",
                    label="Whitelist",
                )
            )
            defs.append(
                TextDef(
                    f"{prefix}blacklist",
                    default="",
                    label="Blacklist",
                )
            )
            defs.append(
                TextDef(
                    f"{prefix}initial_status",
                    default="Active",
                    label="Initial Status",
                )
            )
        return defs

    def process(self, instance):
        context = instance.context

        # Determine job type from instance family
        family = instance.data.get("family")
        if family in self.families:
            job_type = family
        else:
            job_type = "render"  # fallback

        # Load profiles
        profile_list = context.data.get("deadline_profiles", [])
        if not profile_list:
            # Try to get from settings
            profile_list = self.get_plugin_settings_from_profile(context)

        # Migrate old profiles
        migrated = [profiles.migrate_old_profile(p) for p in profile_list]

        # Find matching profile
        matching_profile = None
        for profile in migrated:
            if profile.get("job_type") == job_type:
                # Check other filters (product types, task types, etc.)
                # For simplicity, assume exact match
                matching_profile = profile
                break

        if not matching_profile:
            # Use default
            default_profiles = profiles.get_default_job_type_profiles()
            matching_profile = default_profiles.get(job_type, default_profiles["render"])

        # Apply settings to instance with prefix
        prefix = f"{job_type}_"
        for key, value in matching_profile.items():
            if key == "job_type":
                continue
            prefixed_key = f"{prefix}{key}"
            # Also check for non-prefixed key for backward compatibility
            if prefixed_key in instance.data.get("publish_attributes", {}):
                # Use stored override if present
                continue
            # Set default if not already set
            instance.data.setdefault(prefixed_key, value)

        # Also convert old non-prefixed attributes for backward compatibility
        old_keys = ["priority", "chunk_size", "pool", "group", "department",
                    "machine_list", "limit_groups", "concurrent_tasks",
                    "enforce_limit_groups", "on_complete", "suspended",
                    "whitelist", "blacklist", "initial_status"]
        for key in old_keys:
            if key in instance.data:
                prefixed_key = f"{prefix}{key}"
                if prefixed_key not in instance.data:
                    instance.data[prefixed_key] = instance.data.pop(key)
