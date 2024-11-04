import os
from datetime import datetime

import attr
import pyblish.api
from ayon_core.lib import (
    is_in_tests,
)
from ayon_core.pipeline import (
    AYONPyblishPluginMixin
)
from ayon_deadline import abstract_submit_deadline


@attr.s
class HoudiniPluginInfo(object):
    Build = attr.ib(default=None)
    IgnoreInputs = attr.ib(default=True)
    ScriptJob = attr.ib(default=True)
    SceneFile = attr.ib(default=None)   # Input
    SaveFile = attr.ib(default=True)
    ScriptFilename = attr.ib(default=None)
    OutputDriver = attr.ib(default=None)
    Version = attr.ib(default=None)  # Mandatory for Deadline
    ProjectPath = attr.ib(default=None)


class HoudiniCacheSubmitDeadline(abstract_submit_deadline.AbstractSubmitDeadline,   # noqa
                                 AYONPyblishPluginMixin):
    """Submit Houdini scene to perform a local publish in Deadline.

    Publishing in Deadline can be helpful for scenes that publish very slow.
    This way it can process in the background on another machine without the
    Artist having to wait for the publish to finish on their local machine.
    """

    label = "Submit Scene to Deadline"
    order = pyblish.api.IntegratorOrder
    hosts = ["houdini"]
    families = ["publish.hou"]
    targets = ["local"]
    settings_category = "deadline"

    def get_job_info(self, job_info=None):
        instance = self._instance
        context = instance.context
        assert all(
            result["success"] for result in context.data["results"]
        ), "Errors found, aborting integration.."

        project_name = instance.context.data["projectName"]
        filepath = context.data["currentFile"]
        scenename = os.path.basename(filepath)
        job_name = "{scene} - {instance} [PUBLISH]".format(
            scene=scenename, instance=instance.name)
        batch_name = "{code} - {scene}".format(
            code=project_name, scene=scenename)
        if is_in_tests():
            batch_name += datetime.now().strftime("%d%m%Y%H%M%S")

        job_info.Name = job_name
        job_info.BatchName = batch_name
        job_info.Plugin = instance.data.get("plugin") or "Houdini"

        rop_node = self.get_rop_node(instance)
        if rop_node.type().name() != "alembic":
            frames = "{start}-{end}x{step}".format(
                start=int(instance.data["frameStart"]),
                end=int(instance.data["frameEnd"]),
                step=int(instance.data["byFrameStep"]),
            )

            job_info.Frames = frames

        return job_info

    def get_plugin_info(self):
        # Not all hosts can import this module.
        import hou

        instance = self._instance
        version = hou.applicationVersionString()
        version = ".".join(version.split(".")[:2])
        rop = self.get_rop_node(instance)
        plugin_info = HoudiniPluginInfo(
            Build=None,
            IgnoreInputs=True,
            ScriptJob=True,
            SceneFile=self.scene_path,
            SaveFile=True,
            OutputDriver=rop.path(),
            Version=version,
            ProjectPath=os.path.dirname(self.scene_path)
        )

        plugin_payload = attr.asdict(plugin_info)

        return plugin_payload

    def process(self, instance):
        super(HoudiniCacheSubmitDeadline, self).process(instance)
        output_dir = os.path.dirname(instance.data["files"][0])
        instance.data["outputDir"] = output_dir
        instance.data["toBeRenderedOn"] = "deadline"

    def get_rop_node(self, instance):
        # Not all hosts can import this module.
        import hou

        rop = instance.data.get("instance_node")
        rop_node = hou.node(rop)

        return rop_node
