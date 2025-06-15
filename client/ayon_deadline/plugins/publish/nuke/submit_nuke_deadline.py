import os
import re
from dataclasses import dataclass, field, asdict
import copy

import pyblish.api

from ayon_core.lib import BoolDef
from ayon_core.pipeline.publish import (
    AYONPyblishPluginMixin
)
from ayon_deadline import abstract_submit_deadline


@dataclass
class NukePluginInfo:
    SceneFile: str = field(default=None)   # Input
    Version: str = field(default=None)    # Mandatory for Deadline
    ProjectPath: str = field(default=None)
    OutputFilePath: str = field(default=None)
    UseGpu: bool = field(default=True)
    WriteNode: str = field(default=None)


class NukeSubmitDeadline(
    abstract_submit_deadline.AbstractSubmitDeadline,
    AYONPyblishPluginMixin
):
    """Submit write to Deadline

    Renders are submitted to a Deadline Web Service as
    supplied via settings key "DEADLINE_REST_URL".

    """

    label = "Submit Nuke to Deadline"
    order = pyblish.api.IntegratorOrder + 0.1
    hosts = ["nuke"]
    families = ["render", "prerender"]
    optional = True
    targets = ["local"]
    settings_category = "deadline"

    use_gpu = None
    node_class_limit_groups = {}

    def process(self, instance):
        """Plugin entry point."""
        if not instance.data.get("farm"):
            self.log.debug("Should not be processed on farm, skipping.")
            return

        self._instance = instance

        context = instance.context
        self._deadline_url = instance.data["deadline"]["url"]
        assert self._deadline_url, "Requires Deadline Webservice URL"

        # adding expected files to instance.data
        write_node = instance.data["transientData"]["node"]
        render_path = instance.data["path"]
        start_frame = int(instance.data["frameStartHandle"])
        end_frame = int(instance.data["frameEndHandle"])
        self._expected_files(
            instance,
            render_path,
            start_frame,
            end_frame
        )

        job_info = self.get_generic_job_info(instance)
        self.job_info = self.get_job_info(job_info=job_info)

        self._set_scene_path(
            context.data["currentFile"],
            job_info.use_published,
            instance.data.get("stagingDir_is_custom", False)
        )

        self._append_job_output_paths(
            instance,
            self.job_info
        )

        self.plugin_info = self.get_plugin_info(
            scene_path=self.scene_path,
            render_path=render_path,
            write_node_name=write_node.name()
        )

        self.aux_files = self.get_aux_files()

        plugin_info_data = instance.data["deadline"]["plugin_info_data"]
        if plugin_info_data:
            self.apply_additional_plugin_info(plugin_info_data)

        if instance.data["render_target"] != "frames_farm":
            job_id = self.process_submission()
            self.log.info("Submitted job to Deadline: {}.".format(job_id))

            render_path = instance.data["path"]
            instance.data["outputDir"] = os.path.dirname(
                render_path).replace("\\", "/")

        if instance.data.get("bakingNukeScripts"):
            for baking_script in instance.data["bakingNukeScripts"]:
                self.job_info = copy.deepcopy(self.job_info)
                self.job_info.JobType = "Normal"

                response_data = instance.data.get("deadlineSubmissionJob", {})
                # frames_farm instance doesn't have render submission
                if response_data.get("_id"):
                    self.job_info.BatchName = response_data["Props"]["Batch"]
                    self.job_info.JobDependencies.append(response_data["_id"])

                render_path = baking_script["bakeRenderPath"]
                scene_path = baking_script["bakeScriptPath"]
                write_node_name = baking_script["bakeWriteNodeName"]

                self.job_info.Name = os.path.basename(render_path)

                # baking job shouldn't be split
                self.job_info.ChunkSize = 999999

                self.job_info.Frames = f"{start_frame}-{end_frame}"

                self.plugin_info = self.get_plugin_info(
                    scene_path=scene_path,
                    render_path=render_path,
                    write_node_name=write_node_name
                )
                job_id = self.process_submission()
                self.log.info(
                    "Submitted baking job to Deadline: {}.".format(job_id))

                # add to list of job Id
                if not instance.data.get("bakingSubmissionJobs"):
                    instance.data["bakingSubmissionJobs"] = []

                instance.data["bakingSubmissionJobs"].append(job_id)

    def get_job_info(self, job_info=None, **kwargs):
        instance = self._instance

        job_info.Plugin = "Nuke"

        start_frame = int(instance.data["frameStartHandle"])
        end_frame = int(instance.data["frameEndHandle"])
        # already collected explicit values for rendered Frames
        if not job_info.Frames:
            job_info.Frames = "{start}-{end}".format(
                start=start_frame,
                end=end_frame
            )
        limit_groups = self._get_limit_groups(self.node_class_limit_groups)
        job_info.LimitGroups.extend(limit_groups)

        render_path = instance.data["path"]
        job_info.Name = os.path.basename(render_path)

        return job_info

    def get_plugin_info(
            self, scene_path=None, render_path=None, write_node_name=None):
        instance = self._instance
        context = instance.context
        version = re.search(r"\d+\.\d+", context.data.get("hostVersion"))

        attribute_values = self.get_attr_values_from_data(instance.data)

        render_dir = os.path.dirname(render_path)
        plugin_info = NukePluginInfo(
            SceneFile=scene_path,
            Version=version.group(),
            OutputFilePath=render_dir.replace("\\", "/"),
            ProjectPath=scene_path,
            UseGpu=attribute_values["use_gpu"],
            WriteNode=write_node_name
        )

        plugin_payload: dict = asdict(plugin_info)
        return plugin_payload

    @classmethod
    def get_attribute_defs(cls):
        return [
            BoolDef(
                "use_gpu",
                label="Use GPU",
                default=cls.use_gpu,
            ),
        ]

    def _get_limit_groups(self, limit_groups):
        """Search for limit group nodes and return group name.
        Limit groups will be defined as pairs in Nuke deadline submitter
        presents where the key will be name of limit group and value will be
        a list of plugin's node class names. Thus, when a plugin uses more
        than one node, these will be captured and the triggered process
        will add the appropriate limit group to the payload jobinfo attributes.
        Returning:
            list: captured groups list
        """
        # Not all hosts can import this module.
        import nuke

        captured_groups = []
        for limit_group in limit_groups:
            lg_name = limit_group["name"]

            for node_class in limit_group["value"]:
                for node in nuke.allNodes(recurseGroups=True):
                    # ignore all nodes not member of defined class
                    if node.Class() not in node_class:
                        continue
                    # ignore all disabled nodes
                    if node["disable"].value():
                        continue
                    # add group name if not already added
                    if lg_name not in captured_groups:
                        captured_groups.append(lg_name)
        return captured_groups

    def _expected_files(
        self,
        instance,
        filepath,
        start_frame,
        end_frame
    ):
        """ Create expected files in instance data
        """
        if instance.data["render_target"] == "frames_farm":
            self.log.debug(
                "Expected files already collected for 'frames_farm', skipping."
            )
            return

        if not instance.data.get("expectedFiles"):
            instance.data["expectedFiles"] = []

        dirname = os.path.dirname(filepath)
        file = os.path.basename(filepath)

        # since some files might be already tagged as publish_on_farm
        # we need to avoid adding them to expected files since those would be
        # duplicated into metadata.json file
        representations = instance.data.get("representations", [])
        # check if file is not in representations with publish_on_farm tag
        for repre in representations:
            # Skip if 'publish_on_farm' not available
            if "publish_on_farm" not in repre.get("tags", []):
                continue

            # in case where single file (video, image) is already in
            # representation file. Will be added to expected files via
            # submit_publish_job.py
            if file in repre.get("files", []):
                self.log.debug(
                    "Skipping expected file: {}".format(filepath))
                return

        # in case path is hashed sequence expression
        # (e.g. /path/to/file.####.png)
        if "#" in file:
            pparts = file.split("#")
            padding = "%0{}d".format(len(pparts) - 1)
            file = pparts[0] + padding + pparts[-1]

        # in case input path was single file (video or image)
        if "%" not in file:
            instance.data["expectedFiles"].append(filepath)
            return

        # shift start frame by 1 if slate is present
        if instance.data.get("slate"):
            start_frame -= 1

        # add sequence files to expected files
        for i in range(start_frame, (end_frame + 1)):
            instance.data["expectedFiles"].append(
                os.path.join(dirname, (file % i)).replace("\\", "/"))
