import os
from dataclasses import dataclass, field, asdict
import pyblish.api
from datetime import datetime
from pathlib import Path

from ayon_core.lib import is_in_tests

from ayon_deadline import abstract_submit_deadline


@dataclass
class DeadlinePluginInfo:
    ProjectFile: str = field(default=None)
    EditorExecutableName: str = field(default=None)
    EngineVersion: str = field(default=None)
    CommandLineMode: str = field(default=True)
    OutputFilePath: str = field(default=None)
    Output: str = field(default=None)
    StartupDirectory: str = field(default=None)
    CommandLineArguments: str = field(default=None)
    PerforceStream: str = field(default=None)
    PerforceChangelist: str = field(default=None)
    PerforceGamePath: str = field(default=None)


class UnrealSubmitDeadline(
    abstract_submit_deadline.AbstractSubmitDeadline
):
    """Supports direct rendering of prepared Unreal project on Deadline
    (`render` product must be created with flag for Farm publishing) OR
    Perforce assisted rendering.

    For this Ayon server must contain `ayon-perforce` addon and provide
    configuration for it (P4 credentials etc.)!
    """

    label = "Submit Unreal to Deadline"
    order = pyblish.api.IntegratorOrder + 0.1
    hosts = ["unreal"]
    families = ["render.farm"]  # cannot be "render' as that is integrated
    targets = ["local"]

    def get_job_info(self, job_info=None):
        instance = self._instance

        job_info.BatchName = self._get_batch_name()
        job_info.Plugin = "UnrealEngine5"

        # already collected explicit values for rendered Frames
        if (
            not job_info.Frames
            and instance.data["frameEnd"] > instance.data["frameStart"]
        ):
            # Deadline requires integers in frame range
            frame_range = "{}-{}".format(
                int(round(instance.data["frameStart"])),
                int(round(instance.data["frameEnd"])))
            job_info.Frames = frame_range

        return job_info

    def get_plugin_info(self):
        deadline_plugin_info = DeadlinePluginInfo()

        render_path = self._instance.data["expectedFiles"][0]
        self._instance.data["outputDir"] = os.path.dirname(render_path)
        self._instance.context.data["version"] = 1  #TODO

        render_dir = os.path.dirname(render_path)
        file_name = self._instance.data["file_names"][0]
        render_path = os.path.join(render_dir, file_name)

        deadline_plugin_info.ProjectFile = self.scene_path
        deadline_plugin_info.Output = render_path.replace("\\", "/")

        deadline_plugin_info.EditorExecutableName = "UnrealEditor-Cmd.exe"
        deadline_plugin_info.EngineVersion = self._instance.data["app_version"]
        master_level = self._instance.data["master_level"]
        render_queue_path = self._instance.data["render_queue_path"]
        cmd_args = [
            master_level,
            "-game",
            f"-MoviePipelineConfig={render_queue_path}",
            "-windowed",
            "-Log",
            "-StdOut",
            "-allowStdOutLogVerbosity",
            "-Unattended",
        ]
        self.log.debug(f"cmd-args::{cmd_args}")
        deadline_plugin_info.CommandLineArguments = " ".join(cmd_args)

        # if Perforce - triggered by active `changelist_metadata` instance!!
        collected_perforce = self._get_perforce_info()
        if collected_perforce:
            perforce_data = (
                self._instance.context.data.get("perforce")
                or self._instance.context.data.get("version_control")
            )
            workspace_dir = perforce_data["workspace_dir"]
            stream = perforce_data["stream"]
            self._update_perforce_data(
                self.scene_path,
                workspace_dir,
                stream,
                collected_perforce["change_info"]["change"],
                deadline_plugin_info,
            )

        return asdict(deadline_plugin_info)

    def from_published_scene(self, replace_in_path=True):
        """ Do not overwrite expected files.

            Use published is set to True, so rendering will be triggered
            from published scene (in 'publish' folder). Default implementation
            of abstract class renames expected (eg. rendered) files accordingly
            which is not needed here.
        """
        return super().from_published_scene(False)

    def _get_batch_name(self):
        """Returns value that differentiate jobs in DL.

        For automatic tests it adds timestamp, for Perforce driven change list
        """
        batch_name = os.path.basename(self._instance.data["source"])
        if is_in_tests():
            batch_name += datetime.now().strftime("%d%m%Y%H%M%S")
        collected_perforce = self._get_perforce_info()
        if collected_perforce:
            change = (collected_perforce["change_info"]["change"])
            batch_name = f"{batch_name}_{change}"
        return batch_name

    def _get_perforce_info(self):
        """Look if changelist_metadata is published to get change list info.

        Context perforce dict contains universal connection info, instance
        perforce contains detail about change list.
        """
        change_list_version = {}
        for inst in self._instance.context:
            # get change info from `changelist_metadata` instance
            inst_data = inst.data
            change_list_version = (
                inst_data.get("perforce")
                or inst_data.get("version_control")  # backward compatibility
            )
            if change_list_version:
                context_version = (
                    self._instance.context.data.get("perforce")
                    or self._instance.context.data.get("version_control")
                )
                change_list_version.update(context_version)
                break
        return change_list_version

    def _update_perforce_data(
        self,
        scene_path,
        workspace_dir,
        stream,
        change_list_id,
        deadline_plugin_info,
    ):
        """Adds Perforce metadata which causes DL pre job to sync to change.

        It triggers only in presence of activated `changelist_metadata`
        instance, which materialize info about commit. Artists could return
        to any published commit and re-render if they choose.
        `changelist_metadata` replaces `workfile` as there are no versioned
        Unreal projects (because of size).
        """
        # normalize paths, c:/ vs C:/
        scene_path = str(Path(scene_path).resolve())
        workspace_dir = str(Path(workspace_dir).resolve())

        unreal_project_file_name = os.path.basename(scene_path)

        unreal_project_hierarchy = self.scene_path.replace(workspace_dir, "")
        unreal_project_hierarchy = (
            unreal_project_hierarchy.replace(unreal_project_file_name, ""))
        # relative path from workspace dir to last folder
        unreal_project_hierarchy = unreal_project_hierarchy.strip("\\")

        deadline_plugin_info.ProjectFile = unreal_project_file_name

        deadline_plugin_info.PerforceStream = stream
        deadline_plugin_info.PerforceChangelist = change_list_id
        deadline_plugin_info.PerforceGamePath = unreal_project_hierarchy
