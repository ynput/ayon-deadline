import os
from datetime import datetime

from dataclasses import dataclass, field, asdict
import pyblish.api
from ayon_core.lib import (
    is_in_tests,
)
from ayon_core.pipeline import (
    tempdir,
    AYONPyblishPluginMixin
)
from ayon_core.pipeline.publish.lib import (
    replace_with_published_scene_path
)
from ayon_deadline import abstract_submit_deadline


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
    families = ["publish.farm"]
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
        batch_name = f"{project_name} - {scenename}"
        if is_in_tests():
            batch_name += datetime.now().strftime("%d%m%Y%H%M%S")

        job_info.Name = job_name
        job_info.BatchName = batch_name
        job_info.Plugin = instance.data.get("plugin", "MayaBatch")

        # When `frames` instance data is a string, it indicates that
        #  the output is a single file.
        # Set the chunk size to a large number because multiple
        #  machines cannot render to the same file.
        if isinstance(instance.data.get("frames"), str):
            job_info.ChunkSize = 99999999

        job_info.EnvironmentKeyValue["AYON_REMOTE_PUBLISH"] = "1"
        return job_info

    def get_plugin_info(self):
        # Not all hosts can import this module.
        from maya import cmds
        instance = self._instance
        scene_file = instance.context.data["currentFile"]
        remote_publish_filepath = self.get_remote_publish_script(
            instance)

        plugin_info = MayaPluginInfo(
            ScriptJob=True,
            SceneFile=scene_file,
            ScriptFilename=remote_publish_filepath,
            Version=cmds.about(version=True),
            ProjectPath=cmds.workspace(query=True,
                                       rootDirectory=True)
        )

        plugin_payload = asdict(plugin_info)

        return plugin_payload

    def from_published_scene(self, replace_in_path=False):
        instance = self._instance
        replace_in_path = False
        return replace_with_published_scene_path(
            instance, replace_in_path)

    def process(self, instance):
        super(MayaCacheSubmitDeadline, self).process(instance)
        instance.data["toBeRenderedOn"] = "deadline"

    def get_remote_publish_script(self, instance):
        """Get filepath of the remote publish script for
        ScriptFilename parameter in Job Info

        Args:
            instance (pyblish.api.Instance): Instance

        Returns:
            str: filepath of remote publish script
        """
        temp_dir = tempdir.get_temp_dir(
            instance.context.data["projectName"],
            use_local_temp=True)
        remote_publish_filename = os.path.join(temp_dir, "remote_publish.py")
        with open(remote_publish_filename, "w") as script_file:
            remote_publish_script = self._remote_publish_script()
            script_file.write(remote_publish_script)
        return remote_publish_filename

    def _remote_publish_script(self):
        """
        Script which executes remote publish
        """
        return ("""
try:
    from ayon_core.lib import Logger
    import pyblish.util
except ImportError as exc:
    # Ensure Deadline fails by output an error that contains "Fatal Error:"
    raise ImportError(f"Fatal Error: {{}}".format(exc))

def remote_publish(log):

    # Error exit as soon as any error occurs.
    error_format = "Failed {{plugin.__name__}}: {{error}}:{{error.traceback}}"

    for result in pyblish.util.publish_iter():
        if not result["error"]:
            continue

        error_message = error_format.format(**result)
        log.error(error_message)
        # 'Fatal Error: ' is because of Deadline
        raise RuntimeError("Fatal Error: {{}}".format(error_message))

if __name__ == "__main__":
    # Perform remote publish with thorough error checking
    log = Logger.get_logger(__name__)
    remote_publish(log)

""")
