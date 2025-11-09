import os
from datetime import datetime

from dataclasses import dataclass, field, asdict
import pyblish.api
from ayon_core.lib import (
    is_in_tests,
)

from ayon_core.pipeline import (
    AYONPyblishPluginMixin
)

from ayon_deadline import abstract_submit_deadline

from ayon_deadline.scripts import remote_publish


@dataclass
class MayaPluginInfo:
    ScriptJob: bool = field(default=True)
    SceneFile: bool = field(default=None)   # Input
    ScriptFilename: str = field(default=None)
    Version: str = field(default=None)  # Mandatory for Deadline
    ProjectPath: str = field(default=None)


class MayaCacheSubmitDeadline(abstract_submit_deadline.AbstractSubmitDeadline,   # noqa
                              AYONPyblishPluginMixin):
    """Submit Maya scene to perform a local publish in Deadline.

    Publishing in Deadline can be helpful for scenes that publish very slow.
    This way it can process in the background on another machine without the
    Artist having to wait for the publish to finish on their local machine.
    """

    label = "Submit Scene to Deadline (Maya)"
    order = pyblish.api.IntegratorOrder
    hosts = ["maya"]
    families = ["remote_publish_on_farm"]
    targets = ["local"]
    settings_category = "deadline"

    def get_job_info(self, job_info=None):
        instance = self._instance
        context = instance.context

        project_name = instance.context.data["projectName"]
        filepath = context.data["currentFile"]
        scenename = os.path.basename(filepath)
        job_name = "{scene} - {instance} [PUBLISH]".format(
            scene=scenename, instance=instance.name)
        batch_name = f"{project_name} - {scenename}"
        if is_in_tests():
            batch_name += datetime.now().strftime("%d%m%Y%H%M%S")

        job_info.Name = job_name
        job_info.BatchName = batch_name
        job_info.Plugin = "MayaBatch"
        job_info.ChunkSize = 99999999

        job_info.EnvironmentKeyValue["INSTANCE_IDS"] = instance.name
        job_info.EnvironmentKeyValue["AYON_REMOTE_PUBLISH"] = "1"
        return job_info

    def get_plugin_info(self):
        # Not all hosts can import this module.
        from maya import cmds
        instance = self._instance
        scene_file = instance.context.data["currentFile"]

        plugin_info = MayaPluginInfo(
            ScriptJob=True,
            SceneFile=scene_file,
            ScriptFilename=remote_publish.__file__.replace(".pyc", ".py"),
            Version=cmds.about(version=True),
            ProjectPath=cmds.workspace(query=True,
                                       rootDirectory=True)
        )

        plugin_payload = asdict(plugin_info)

        return plugin_payload

    def from_published_scene(self, replace_in_path=False):
        return super().from_published_scene(False)

    def process(self, instance):
        super(MayaCacheSubmitDeadline, self).process(instance)
        instance.data["toBeRenderedOn"] = "deadline"

