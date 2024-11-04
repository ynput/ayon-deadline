# -*- coding: utf-8 -*-
import json

import pyblish.api
from ayon_core.lib import (
    BoolDef,
    NumberDef,
    TextDef,
    UISeparatorDef
)
from ayon_core.pipeline.publish import AYONPyblishPluginMixin
from ayon_core.lib.profiles_filtering import filter_profiles

from ayon_deadline.lib import (
    FARM_FAMILIES,
    AYONDeadlineJobInfo,
)


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

    profiles = []

    def process(self, instance):
        attr_values = self._get_jobinfo_defaults(instance)

        attr_values.update(self.get_attr_values_from_data(instance.data))
        job_info = AYONDeadlineJobInfo.from_dict(attr_values)

        self._handle_machine_list(attr_values, job_info)

        self._handle_additional_jobinfo(attr_values, job_info)

        instance.data["deadline"]["job_info"] = job_info

        # pass through explicitly key and values for PluginInfo
        plugin_info_data = None
        if attr_values["additional_plugin_info"]:
            plugin_info_data = (
                json.loads(attr_values["additional_plugin_info"]))
        instance.data["deadline"]["plugin_info_data"] = plugin_info_data

    def _handle_additional_jobinfo(self,attr_values, job_info):
        """Adds not explicitly implemented fields by values from Settings."""
        additional_job_info = attr_values["additional_job_info"]
        if not additional_job_info:
            return
        for key, value in json.loads(additional_job_info).items():
            setattr(job_info, key, value)

    def _handle_machine_list(self, attr_values, job_info):
        machine_list = attr_values["machine_list"]
        if machine_list:
            if job_info.MachineListDeny:
                job_info.Blacklist = machine_list
            else:
                job_info.Whitelist = machine_list

    @classmethod
    def apply_settings(cls, project_settings):
        settings = project_settings["deadline"]
        profiles = settings["publish"][cls.__name__]["profiles"]

        cls.profiles = profiles or []

    @classmethod
    def get_attr_defs_for_instance(cls, create_context, instance):
        host_name = create_context.host_name

        task_name = instance["task"]
        folder_path = instance["folderPath"]
        task_entity = create_context.get_task_entity(folder_path, task_name)

        task_name = task_type = None
        if task_entity:
            task_name = task_entity["name"]
            task_type = task_entity["taskType"]
        profile = filter_profiles(
            cls.profiles,
            {
                "host_names": host_name,
                "task_types": task_type,
                "task_names": task_name,
                # "product_type": product_type
            }
        )
        if not profile:
            return []
        overrides = set(profile["overrides"])
        if not overrides:
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

        defs = cls._host_specific_attr_defs(create_context, instance, defs)

        defs.append(
            UISeparatorDef("deadline_defs_end")
        )

        return defs

    @classmethod
    def _get_artist_overrides(cls, overrides, profile):
        """Provide list of Defs that could be filled by artist"""
        # should be matching to extract_jobinfo_overrides_enum
        default_values = {}
        for key in overrides:
            default_value = profile[key]
            if isinstance(default_value, list):
                default_value = ",".join(default_value)
            default_values[key] = default_value

        override_defs = [
            NumberDef(
                "chunk_size",
                label="Frames Per Task",
                default=default_values["chunk_size"],
                decimals=0,
                minimum=1,
                maximum=1000
            ),
            NumberDef(
                "priority",
                label="Priority",
                default=default_values["priority"],
                decimals=0
            ),
            TextDef(
                "department",
                label="Department",
                default=default_values["department"]
            ),
            TextDef(
                "limit_groups",
                label="Limit Groups",
                # multiline=True,  TODO - some DCC might have issues with storing multi lines
                default=default_values["limit_groups"],
                placeholder="machine1,machine2"
            ),
            TextDef(
                "job_delay",
                label="Delay job (timecode dd:hh:mm:ss)",
                default=default_values["job_delay"],
            )
        ]

        return override_defs

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

        profile = filter_profiles(
            profiles,
            {
                "host_names": host_name,
                "task_types": task_type,
                "task_names": task_name,
                # "product_type": product_type
            }
        )
        return profile or {}

    @classmethod
    def _host_specific_attr_defs(cls, create_context, instance, defs):

        host_name = create_context.host_name
        if host_name == "maya":
            defs.extend([
                NumberDef(
                    "tile_priority",
                    label="Tile Assembler Priority",
                    decimals=0,
                ),
                BoolDef(
                    "strict_error_checking",
                    label="Strict Error Checking",
                ),
            ])

        return defs
