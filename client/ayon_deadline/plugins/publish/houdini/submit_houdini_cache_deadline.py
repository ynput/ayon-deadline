import os
import getpass
from datetime import datetime

import attr
import pyblish.api
from ayon_core.lib import (
    TextDef,
    NumberDef,
    is_in_tests,
)
from ayon_core.pipeline import (
    AYONPyblishPluginMixin
)
from ayon_deadline import abstract_submit_deadline
from ayon_deadline.abstract_submit_deadline import DeadlineJobInfo


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

    priority = 50
    chunk_size = 999999
    group = None
    limits = ""
    machine_limit = 0
    jobInfo = {}
    pluginInfo = {}


    def get_job_info(self):
        job_info = DeadlineJobInfo(Plugin="Houdini")

        job_info.update(self.jobInfo)
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
        batch_name = "{code} - {scene}".format(code=project_name,
                                               scene=scenename)
        if is_in_tests():
            batch_name += datetime.now().strftime("%d%m%Y%H%M%S")

        job_info.Name = job_name
        job_info.BatchName = batch_name
        job_info.Plugin = instance.data["plugin"]
        job_info.UserName = context.data.get("deadlineUser", getpass.getuser())

        frames = "{start}-{end}x{step}".format(
            start=int(instance.data["frameStart"]),
            end=int(instance.data["frameEnd"]),
            step=int(instance.data["byFrameStep"]),
        )

        job_info.Frames = frames

        job_info.Pool = instance.data.get("primaryPool")
        job_info.SecondaryPool = instance.data.get("secondaryPool")

        attr_values = self.get_attr_values_from_data(instance.data)

        chunk_size = instance.data.get("chunk_size", self.chunk_size)
        # When `frames` instance data is a string, it indicates that
        #  the output is a single file.
        # Set the chunk size to a large number because multiple
        #  machines cannot render to the same file.
        if isinstance(instance.data.get("frames"), str):
            chunk_size = 99999999

        job_info.ChunkSize = chunk_size

        job_info.Comment = context.data.get("comment")
        job_info.Priority = attr_values.get("priority", self.priority)
        job_info.Group = attr_values.get("group", self.group)
        job_info.LimitGroups = attr_values.get("limits", self.limits)
        job_info.MachineLimit = attr_values.get(
            "machine_limit", self.machine_limit
        )

        # Set job environment variables
        job_info.add_instance_job_env_vars(self._instance)
        job_info.add_render_job_env_var()

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

    @classmethod
    def get_attribute_defs(cls):
        defs = super(HoudiniCacheSubmitDeadline, cls).get_attribute_defs()
        defs.extend([
            NumberDef("priority",
                      minimum=1,
                      maximum=250,
                      decimals=0,
                      default=cls.priority,
                      label="Priority"),
            TextDef("group",
                    default=cls.group,
                    label="Group Name"),
            TextDef(
                "limits",
                default=cls.limits,
                label="Limit Groups",
                placeholder="value1,value2",
                tooltip="Enter a comma separated list of limit groups."
            ),
            NumberDef(
                "machine_limit",
                default=cls.machine_limit,
                label="Machine Limit",
                tooltip="maximum number of machines for this job."
            ),
        ])

        return defs
