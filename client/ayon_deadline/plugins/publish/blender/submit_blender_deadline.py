# -*- coding: utf-8 -*-
"""Submitting render job to Deadline."""

import os
from dataclasses import dataclass, field, asdict

from ayon_core.pipeline.publish import AYONPyblishPluginMixin
from ayon_core.pipeline.farm.tools import iter_expected_files

from ayon_deadline import abstract_submit_deadline


@dataclass
class BlenderPluginInfo:
    SceneFile: str = field(default=None)   # Input
    Version: str = field(default=None)  # Mandatory for Deadline
    SaveFile: bool = field(default=True)


class BlenderSubmitDeadline(abstract_submit_deadline.AbstractSubmitDeadline,
                            AYONPyblishPluginMixin):
    label = "Submit Render to Deadline"
    hosts = ["blender"]
    families = ["render"]  # TODO this should be farm specific as render.farm
    settings_category = "deadline"

    def get_job_info(self, job_info=None):
        instance = self._instance
        job_info.Plugin = instance.data.get("blenderRenderPlugin", "Blender")

        # Deadline requires integers in frame range
        frames = "{start}-{end}x{step}".format(
            start=int(instance.data["frameStartHandle"]),
            end=int(instance.data["frameEndHandle"]),
            step=int(instance.data["byFrameStep"]),
        )
        job_info.Frames = frames

        return job_info

    def get_plugin_info(self):
        # Not all hosts can import this module.
        import bpy

        plugin_info = BlenderPluginInfo(
            SceneFile=self.scene_path,
            Version=bpy.app.version_string,
            SaveFile=True,
        )

        plugin_payload = asdict(plugin_info)

        return plugin_payload

    def process_submission(self, auth=None):
        instance = self._instance

        expected_files = instance.data["expectedFiles"]
        if not expected_files:
            raise RuntimeError("No Render Elements found!")

        first_file = next(iter_expected_files(expected_files))
        output_dir = os.path.dirname(first_file)
        instance.data["outputDir"] = output_dir
        instance.data["toBeRenderedOn"] = "deadline"

        payload = self.assemble_payload()
        auth = self._instance.data["deadline"]["auth"]
        verify = self._instance.data["deadline"]["verify"]
        return self.submit(payload, auth=auth, verify=verify)

    def from_published_scene(self):
        """
        This is needed to set the correct path for the json metadata. Because
        the rendering path is set in the blend file during the collection,
        and the path is adjusted to use the published scene, this ensures that
        the metadata and the rendered files are in the same location.
        """
        return super().from_published_scene(False)
