import os
import pyblish.api
from dataclasses import dataclass, field, asdict

from ayon_core.lib import (
    env_value_to_bool,
    collect_frames,
)
from ayon_deadline import abstract_submit_deadline


@dataclass
class DeadlinePluginInfo():
    Comp: str = field(default=None)
    SceneFile: str = field(default=None)
    OutputFilePath: str = field(default=None)
    Output: str = field(default=None)
    StartupDirectory: str = field(default=None)
    Arguments: str = field(default=None)
    ProjectPath: str = field(default=None)
    AWSAssetFile0: str = field(default=None)
    Version: str = field(default=None)
    MultiProcess: bool = field(default=None)


class AfterEffectsSubmitDeadline(
    abstract_submit_deadline.AbstractSubmitDeadline
):

    label = "Submit AE to Deadline"
    order = pyblish.api.IntegratorOrder + 0.1
    hosts = ["aftereffects"]
    families = ["render.farm"]  # cannot be "render' as that is integrated
    use_published = True
    targets = ["local"]
    settings_category = "deadline"

    def get_job_info(self, job_info=None):
        job_info.Plugin = "AfterEffects"

        # already collected explicit values for rendered Frames
        if not job_info.Frames:
            # Deadline requires integers in frame range
            frame_range = "{}-{}".format(
                int(round(self._instance.data["frameStart"])),
                int(round(self._instance.data["frameEnd"])))
            job_info.Frames = frame_range

        return job_info

    def get_plugin_info(self):
        deadline_plugin_info = DeadlinePluginInfo()

        render_path = self._instance.data["expectedFiles"][0]

        file_name, frame = list(collect_frames([render_path]).items())[0]
        if frame:
            # Replace frame ('000001') with Deadline's required '[#######]'
            #   expects filename in format:
            #   'project_folder_product_version.FRAME.ext'
            render_dir = os.path.dirname(render_path)
            file_name = os.path.basename(render_path)
            hashed = '[{}]'.format(len(frame) * "#")
            file_name = file_name.replace(frame, hashed)
            render_path = os.path.join(render_dir, file_name)

        deadline_plugin_info.Comp = self._instance.data["comp_name"]
        deadline_plugin_info.Version = self._instance.data["app_version"]
        # must be here because of DL AE plugin
        # added override of multiprocess by env var, if shouldn't be used for
        # some app variant use MULTIPROCESS:false in Settings, default is True
        deadline_plugin_info.MultiProcess = env_value_to_bool(
            "MULTIPROCESS", default=True
        )
        deadline_plugin_info.SceneFile = self.scene_path
        deadline_plugin_info.Output = render_path.replace("\\", "/")

        return asdict(deadline_plugin_info)

    def from_published_scene(self, replace_in_path=True):
        """ Do not overwrite expected files.

            Use published is set to True, so rendering will be triggered
            from published scene (in 'publish' folder). Default implementation
            of abstract class renames expected (eg. rendered) files accordingly
            which is not needed here.
        """
        return super().from_published_scene(False)
