import sys
from dataclasses import dataclass, field, asdict
import getpass
import pyblish.api
from datetime import datetime
from pathlib import Path

from unreal import MoviePipelineEditorLibrary as mpel

from ayon_core.lib import is_in_tests

import ayon_unreal

from ayon_deadline import abstract_submit_deadline


@dataclass
class DeadlinePluginInfo:
    ProjectFile: str = field(default=None)
    Executable: str = field(default=None)
    EngineVersion: str = field(default=None)
    CommandLineMode: str = field(default=True)
    OutputFilePath: str = field(default=None)
    Output: str = field(default=None)
    StartupDirectory: str = field(default=None)
    CommandLineArguments: str = field(default=None)
    MultiProcess: bool = field(default=None)


class UnrealSubmitDeadline(
    abstract_submit_deadline.AbstractSubmitDeadline
):
    """Supports direct rendering of prepared Unreal project on Deadline
    (`render` product must be created with flag for Farm publishing) OR
    Perforce assisted rendering.

    For this Ayon server must contain `ayon-version-control` addon and provide
    configuration for it (P4 credentials etc.)!
    """

    label = "Submit Unreal to Deadline"
    order = pyblish.api.IntegratorOrder + 0.1
    hosts = ["unreal"]
    families = ["render.farm"]  # cannot be "render' as that is integrated
    targets = ["local"]

    def get_job_info(self, job_info=None):
        instance = self._instance
        context = self._instance.context

        job_info.BatchName = self._get_batch_name()
        job_info.Plugin = "UnrealEngine5"
        job_info.Name = instance.data["name"]
        job_info.Plugin = "UnrealEngine5"
        job_info.UserName = context.data.get(
            "deadlineUser", getpass.getuser())
        job_info.CommandLineMode = False    # enables RPC

        if instance.data["frameEnd"] > instance.data["frameStart"]:
            # Deadline requires integers in frame range
            frame_range = "{}-{}".format(
                int(round(instance.data["frameStart"])),
                int(round(instance.data["frameEnd"])))
            job_info.Frames = frame_range

        if work_mrq := self._instance.data["work_mrq"]:
            job_info.ExtraInfoKeyValue.update(
                {"SerializedMRQ": mpel.convert_manifest_file_to_string(work_mrq)}
            )

        return job_info

    def get_plugin_info(self):
        deadline_plugin_info = DeadlinePluginInfo()

        expected_file = Path(self._instance.data["expectedFiles"][0]).resolve()
        self._instance.data["outputDir"] = expected_file.parent.as_posix()
        self._instance.context.data["version"] = 1  #TODO

        file_name = self._instance.data["file_names"][0]
        render_path = (expected_file.parent / file_name).resolve()

        deadline_plugin_info.ProjectFile = self.scene_path
        deadline_plugin_info.Output = render_path.as_posix()
        deadline_plugin_info.Executable = self._get_executable()
        deadline_plugin_info.EngineVersion = self._instance.data["app_version"]

        pre_render_script = (
            Path(ayon_unreal.__file__).parent / "api" / "rendering_remote.py")
        cmd_args = [f'-execcmds="py {pre_render_script.as_posix()}"']
        if work_mrq := self._instance.data["work_mrq"]:
            manifest: str = Path(work_mrq).as_posix()
            cmd_args.append(f"-MRQManifest={manifest}")
        cmd_args.append("-MRQInstance")
        self.log.debug(f"cmd-args::{cmd_args}")
        deadline_plugin_info.CommandLineArguments = " ".join(cmd_args)

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
        batch_name = Path(self._instance.data["source"]).stem
        if is_in_tests():
            batch_name += datetime.now().strftime("%d%m%Y%H%M%S")

        if p4_data := self._instance.context.data.get("perforce"):
            batch_name += f" - Stream {p4_data['stream']}"
            if cl_num := p4_data.get("changelist"):
                batch_name += f"@{cl_num}"

        return batch_name

    def _get_executable(self):
        """Returns path to Unreal executable.
        """
        curr_ue = Path(sys.executable).resolve()
        ue_cmd_exe = curr_ue.parent / "UnrealEditor-Cmd.exe"
        return ue_cmd_exe.as_posix()

