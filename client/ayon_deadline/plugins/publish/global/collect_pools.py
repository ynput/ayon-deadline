# -*- coding: utf-8 -*-
import pyblish.api
from ayon_core.lib import TextDef
from ayon_core.pipeline.publish import AYONPyblishPluginMixin

from ayon_deadline.lib import FARM_FAMILIES


class CollectDeadlinePools(pyblish.api.InstancePlugin,
                           AYONPyblishPluginMixin):
    """Collect pools from instance or Publisher attributes, from Setting
    otherwise.

    Pools are used to control which DL workers could render the job.

    Pools might be set:
    - directly on the instance (set directly in DCC)
    - from Publisher attributes
    - from defaults from Settings.

    Publisher attributes could be shown even for instances that should be
    rendered locally as visibility is driven by product type of the instance
    (which will be `render` most likely).
    (Might be resolved in the future and class attribute 'families' should
    be cleaned up.)

    """

    order = pyblish.api.CollectorOrder + 0.420
    label = "Collect Deadline Pools"
    hosts = [
        "aftereffects",
        "fusion",
        "harmony",
        "maya",
        "max",
        "houdini",
        "nuke",
        "unreal"
    ]

    families = FARM_FAMILIES

    primary_pool = None
    secondary_pool = None

    @classmethod
    def apply_settings(cls, project_settings):
        # deadline.publish.CollectDeadlinePools
        settings = project_settings["deadline"]["publish"]["CollectDeadlinePools"]  # noqa
        cls.primary_pool = settings.get("primary_pool", None)
        cls.secondary_pool = settings.get("secondary_pool", None)

    def process(self, instance):
        attr_values = self.get_attr_values_from_data(instance.data)
        if not instance.data.get("primaryPool"):
            instance.data["primaryPool"] = (
                attr_values.get("primaryPool") or self.primary_pool or "none"
            )
        if instance.data["primaryPool"] == "-":
            instance.data["primaryPool"] = None

        if not instance.data.get("secondaryPool"):
            instance.data["secondaryPool"] = (
                attr_values.get("secondaryPool") or self.secondary_pool or "none"  # noqa
            )

        if instance.data["secondaryPool"] == "-":
            instance.data["secondaryPool"] = None

    @classmethod
    def get_attr_defs_for_instance(cls, create_context, instance):
        # Filtering of instance, if needed, can be customized
        if not cls.instance_matches_plugin_families(instance):
            return []

        # Attributes logic
        creator_attributes = instance["creator_attributes"]

        visible = creator_attributes.get("farm", False)
        # TODO: Preferably this would be an enum for the user
        #       but the Deadline server URL can be dynamic and
        #       can be set per render instance. Since get_attribute_defs
        #       can't be dynamic unfortunately EnumDef isn't possible (yet?)
        # pool_names = self.deadline_addon.get_deadline_pools(deadline_url,
        #                                                      self.log)
        # secondary_pool_names = ["-"] + pool_names

        return [
            TextDef("primaryPool",
                    label="Primary Pool",
                    default=cls.primary_pool,
                    tooltip="Deadline primary pool, "
                            "applicable for farm rendering",
                    visible=visible),
            TextDef("secondaryPool",
                    label="Secondary Pool",
                    default=cls.secondary_pool,
                    tooltip="Deadline secondary pool, "
                            "applicable for farm rendering",
                    visible=visible)
        ]

    @classmethod
    def register_create_context_callbacks(cls, create_context):
        create_context.add_value_changed_callback(cls.on_values_changed)

    @classmethod
    def on_values_changed(cls, event):
        """Update instance attribute definitions on attribute changes."""

        # Update attributes if any of the following plug-in attributes
        # change:
        keys = ["farm"]

        for instance_change in event["changes"]:
            instance = instance_change["instance"]
            if not cls.instance_matches_plugin_families(instance):
                continue
            value_changes = instance_change["changes"]
            plugin_attribute_changes = (
                value_changes.get("creator_attributes", {})
                .get(cls.__name__, {}))

            if not any(key in plugin_attribute_changes for key in keys):
                continue

            # Update the attribute definitions
            new_attrs = cls.get_attr_defs_for_instance(
                event["create_context"], instance
            )
            instance.set_publish_plugin_attr_defs(cls.__name__, new_attrs)
