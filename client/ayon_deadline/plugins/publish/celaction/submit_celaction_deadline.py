import os
import re
import pyblish.api
from dataclasses import dataclass, field, asdict

from ayon_deadline import abstract_submit_deadline


@dataclass
class CelactionPluginInfo:
    SceneFile: str = field(default=None)
    OutputFilePath: str = field(default=None)
    Output: str = field(default=None)
    StartupDirectory: str = field(default=None)
    Arguments: str = field(default=None)
    ProjectPath: str = field(default=None)
    AWSAssetFile0: str = field(default=None)


class CelactionSubmitDeadline(abstract_submit_deadline.AbstractSubmitDeadline):
    """Submit CelAction2D scene to Deadline

    Renders are submitted to a Deadline Web Service.

    """

    label = "Submit CelAction to Deadline"
    order = pyblish.api.IntegratorOrder + 0.1
    hosts = ["celaction"]
    families = ["render.farm"]

    def get_job_info(self, job_info=None):
        job_info.Plugin = "CelAction"

        # already collected explicit values for rendered Frames
        if not job_info.Frames:
            # Deadline requires integers in frame range
            frame_range = "{}-{}".format(
                int(round(self._instance.data["frameStart"])),
                int(round(self._instance.data["frameEnd"])))
            job_info.Frames = frame_range

        return job_info

    def get_plugin_info(self):
        plugin_info = CelactionPluginInfo()
        instance = self._instance

        render_path = instance.data["path"]
        render_dir = os.path.dirname(render_path)

        self._expected_files(instance, render_path)

        script_path = self.scene_path
        plugin_info.SceneFile = script_path
        plugin_info.ProjectPath = script_path
        plugin_info.OutputFilePath = render_dir.replace("\\", "/")
        plugin_info.StartupDirectory = ""

        resolution_width = instance.data["resolutionWidth"]
        resolution_height = instance.data["resolutionHeight"]
        search_results = re.search(r"(%0)(\d)(d)[._]", render_path).groups()
        split_patern = "".join(search_results)
        padding_number = int(search_results[1])

        args = [
            f"<QUOTE>{script_path}<QUOTE>",
            "-a",
            "-16",
            "-s <STARTFRAME>",
            "-e <ENDFRAME>",
            f"-d <QUOTE>{render_dir}<QUOTE>",
            f"-x {resolution_width}",
            f"-y {resolution_height}",
            f"-r <QUOTE>{render_path.replace(split_patern, '')}<QUOTE>",
            f"-= AbsoluteFrameNumber=on -= PadDigits={padding_number}",
            "-= ClearAttachment=on",
        ]
        plugin_info.Arguments = " ".join(args)

        # adding 2d render specific family for version identification in Loader
        instance.data["families"] = ["render2d"]

        return asdict(plugin_info)

    def _expected_files(self, instance, filepath):
        """ Create expected files in instance data
        """
        if not instance.data.get("expectedFiles"):
            instance.data["expectedFiles"] = []

        dirpath = os.path.dirname(filepath)
        filename = os.path.basename(filepath)

        if "#" in filename:
            pparts = filename.split("#")
            padding = "%0{}d".format(len(pparts) - 1)
            filename = pparts[0] + padding + pparts[-1]

        if "%" not in filename:
            instance.data["expectedFiles"].append(filepath)
            return

        for i in range(self._frame_start, (self._frame_end + 1)):
            instance.data["expectedFiles"].append(
                os.path.join(dirpath, (filename % i)).replace("\\", "/")
            )
