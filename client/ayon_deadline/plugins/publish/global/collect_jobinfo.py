# -*- coding: utf-8 -*-
from collections import OrderedDict

import ayon_api
import pyblish.api
from ayon_core.lib import (
    BoolDef,
    NumberDef,
    TextDef,
    EnumDef,
    is_in_tests,
    UISeparatorDef
)
from ayon_core.pipeline.publish import AYONPyblishPluginMixin
from ayon_core.settings import get_project_settings
from ayon_core.lib.profiles_filtering import filter_profiles

from ayon_deadline.lib import FARM_FAMILIES, DeadlineJobInfo


class CollectJobInfo(pyblish.api.InstancePlugin, AYONPyblishPluginMixin):
    """Collect variables that belong to Deadline's JobInfo.

    Variables like:
    - department
    - priority
    - chunk size

    """

    order = pyblish.api.CollectorOrder + 0.420
    label = "Collect Deadline JobInfo"

    families = FARM_FAMILIES
    targets = ["local"]

    def process(self, instance):
        attr_values = self._get_jobinfo_defaults(instance)

        attr_values.update(self.get_attr_values_from_data(instance.data))
        # do not set empty strings
        attr_values = {
            key: value
            for key,value in attr_values.items()
            if value != ""
        }
        job_info = DeadlineJobInfo.from_dict(attr_values)
        instance.data["deadline"]["job_info"] = job_info

    @classmethod
    def get_attr_defs_for_instance(cls, create_context, instance):
        cls.log.info(create_context.get_current_task_entity())
        if not cls.instance_matches_plugin_families(instance):
            return []

        # will be reworked when CreateContext contains settings and task types
        project_name = create_context.project_name
        project_settings = (
            create_context.get_current_project_settings()
        )

        profiles = (
            project_settings["deadline"]["publish"][cls.__name__]["profiles"])

        if not profiles:
            return []

        host_name = create_context.host_name

        task_name = instance["task"]
        folder_path = instance["folderPath"]
        task_entity = create_context.get_task_entity(folder_path, task_name)

        profile = filter_profiles(
            profiles,
            {
                "host_names": host_name,
                "task_types": task_entity["taskType"],
                "task_names": task_name,
                # "product_type": product_type
            }
        )
        overrides = set(profile["overrides"])
        if not profile or not overrides:
            return []

        defs = [
            UISeparatorDef("deadline_defs_starts"),
        ]

        defs.extend(cls._get_artist_overrides(overrides, profile))

        # explicit
        defs.append(
            TextDef(
                "frames",
                label="Frames",
                default="",
                tooltip="Explicit frames to be rendered. (1, 3-4)"
            )
        )

        defs.append(
            UISeparatorDef("deadline_defs_end")
        )

        return defs

    @classmethod
    def _get_artist_overrides(cls, overrides, profile):
        """Provide list of Defs that could be filled by artist"""
        # should be matching to extract_jobinfo_overrides_enum
        override_defs = [
            NumberDef(
                "chunkSize",
                label="Frames Per Task",
                default=1,
                decimals=0,
                minimum=1,
                maximum=1000
            ),
            NumberDef(
                "priority",
                label="Priority",
                decimals=0
            ),
            TextDef(
                "department",
                label="Department",
                default="",
            ),
            TextDef(
                "limit_groups",
                label="Limit Groups",
                # multiline=True,  TODO - some DCC might have issues with storing multi lines
                default="",
                placeholder="machine1,machine2"
            ),
            TextDef(
                "job_delay",
                label="Delay job (timecode dd:hh:mm:ss)",
                default=""
            )
        ]
        defs = []
        # The Arguments that can be modified by the Publisher
        for attr_def in override_defs:
            if attr_def.key not in overrides:
                continue

            default_value = profile[attr_def.key]
            if (isinstance(attr_def, TextDef) and
                    isinstance(default_value, list)):
                default_value = ",".join(default_value)
            attr_def.default = default_value
            defs.append(attr_def)

        return defs

    @classmethod
    def register_create_context_callbacks(cls, create_context):
        create_context.add_value_changed_callback(cls.on_values_changed)

    @classmethod
    def on_values_changed(cls, event):
        for instance_change in event["changes"]:
            instance = instance_change["instance"]
            if not cls.instance_matches_plugin_families(instance):
                continue
            value_changes = instance_change["changes"]
            if "enabled" not in value_changes:
                continue

            new_attrs = cls.get_attr_defs_for_instance(
                event["create_context"], instance
            )
            instance.set_publish_plugin_attr_defs(cls.__name__, new_attrs)

    def _get_jobinfo_defaults(self, instance):
        """Queries project setting for profile with default values

        Args:
            instance (pyblish.api.Instance): Source instance.

        Returns:
            (dict)
        """
        attr_values = {}

        context_data = instance.context.data
        host_name = context_data["hostName"]
        project_settings = context_data["project_settings"]
        task_entity = context_data["taskEntity"]

        task_name = task_type = None
        if task_entity:
            task_name = task_entity["name"]
            task_type = task_entity["taskType"]
        profiles = (
            project_settings
            ["deadline"]
            ["publish"]
            ["CollectJobInfo"]
            ["profiles"]
        )
        if profiles:
            profile = filter_profiles(
                profiles,
                {
                    "host_names": host_name,
                    "task_types": task_type,
                    "task_names": task_name,
                    # "product_type": product_type
                }
            )
            if profile:
                attr_values = profile
        return attr_values


class CollectMayaJobInfo(CollectJobInfo):
    hosts = [
        "maya",
    ]
    @classmethod
    def get_attr_defs_for_instance(cls, create_context, instance):
        defs = super().get_attr_defs_for_instance(create_context, instance)

        defs.extend([
            NumberDef(
                "tile_priority",
                label="Tile Assembler Priority",
                decimals=0,
                default=cls.tile_priorit
            ),
            BoolDef(
                "strict_error_checking",
                label="Strict Error Checking",
                default=cls.strict_error_checking
            ),
        ])

        return defs
