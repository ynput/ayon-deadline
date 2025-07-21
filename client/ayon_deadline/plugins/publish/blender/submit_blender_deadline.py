# -*- coding: utf-8 -*-
"""Submitting render job to Deadline."""

import os
from dataclasses import dataclass, field, asdict

from ayon_core.pipeline.publish import AYONPyblishPluginMixin, PublishError
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

    def process(self, instance):
        if not instance.data.get("farm"):
            self.log.debug("Render on farm is disabled. "
                           "Skipping deadline submission.")
            return

        # Always set instance output directory to the expected
        expected_files = instance.data["expectedFiles"]
        if not expected_files:
            raise PublishError(
                message="No Render Elements found.",
                title="No Render Elements found.",
                description="Expected files for render elements are empty."
            )

        first_file = next(iter_expected_files(expected_files))
        output_dir = os.path.dirname(first_file)
        instance.data["outputDir"] = output_dir
        instance.data["toBeRenderedOn"] = "deadline"

        # We are submitting a farm job not per instance - but once per Blender
        # scene. This is a hack to avoid submitting multiple jobs for each
        # comp file output because the Deadline job will always render all
        # active ones anyway (and the relevant view layers).
        context = instance.context
        key = f"__hasRun{self.__class__.__name__}"
        if context.data.get(key, False):
            return

        context.data[key] = True

        # Collect all saver instances in context that are to be rendered
        render_instances = []
        for inst in context:
            if inst.data["productType"] != "render":
                # Allow only render instances
                continue

            if not inst.data.get("publish", True):
                # Skip inactive instances
                continue

            if not inst.data.get("farm"):
                # Only consider instances that are also set to be rendered on
                # farm
                continue

            render_instances.append(inst)

        if not render_instances:
            raise PublishError("No instances found for Deadline submission")

        instance.data["_farmRenderInstances"] = render_instances

        super().process(instance)

        # Store the response for dependent job submission plug-ins for all
        # the instances
        transfer_keys = ["deadlineSubmissionJob", "deadline"]
        for render_instance in render_instances:
            for key in transfer_keys:
                render_instance.data[key] = instance.data[key]

        # Remove this data which we only added to get access to the data
        # in the inherited `self.get_job_info()` method.
        instance.data.pop("_farmRenderInstances", None)

    def get_job_info(self, job_info=None, **kwargs):
        instance = self._instance
        job_info.Plugin = instance.data.get("blenderRenderPlugin", "Blender")

        # already collected explicit values for rendered Frames
        if not job_info.Frames:
            # Deadline requires integers in frame range
            frames = "{start}-{end}x{step}".format(
                start=int(instance.data["frameStartHandle"]),
                end=int(instance.data["frameEndHandle"]),
                step=int(instance.data["byFrameStep"]),
            )
            job_info.Frames = frames

        # We override the default behavior of AbstractSubmitDeadline here to
        # include the output directory and output filename for each individual
        # render instance, instead of only the current instance, because we're
        # submitting one job for multiple render instances.
        for render_instance in instance.data["_farmRenderInstances"]:
            if render_instance is instance:
                continue

            self._append_job_output_paths(render_instance, job_info)

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
        payload = self.assemble_payload()
        auth = self._instance.data["deadline"]["auth"]
        verify = self._instance.data["deadline"]["verify"]
        return self.submit(payload, auth=auth, verify=verify)

    def from_published_scene(self, replace_in_path=True):
        """
        This is needed to set the correct path for the json metadata. Because
        the rendering path is set in the blend file during the collection,
        and the path is adjusted to use the published scene, this ensures that
        the metadata and the rendered files are in the same location.
        """
        return super().from_published_scene(False)
