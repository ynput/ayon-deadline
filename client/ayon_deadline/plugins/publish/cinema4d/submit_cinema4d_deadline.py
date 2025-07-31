from dataclasses import dataclass, field, asdict

from ayon_core.pipeline.publish import AYONPyblishPluginMixin
from ayon_deadline import abstract_submit_deadline

import c4d


@dataclass
class Cinema4DPluginInfo:
    SceneFile: str = field(default=None)   # Input
    Version: int = field(default=None)     # Mandatory for Deadline
    Renderer: str = field(default="")
    Take: str = field(default=None)        # Which take to render


class Cinema4DSubmitDeadline(abstract_submit_deadline.AbstractSubmitDeadline,
                             AYONPyblishPluginMixin):
    label = "Submit Render to Deadline"
    hosts = ["cinema4d"]
    families = ["render"]
    settings_category = "deadline"

    def get_job_info(self, job_info=None, **kwargs):
        instance = self._instance
        job_info.Plugin = "Cinema4D"

        # Deadline requires integers in frame range
        job_info.Frames = "{start}-{end}x{step}".format(
            start=int(instance.data["frameStartHandle"]),
            end=int(instance.data["frameEndHandle"]),
            step=int(instance.data["byFrameStep"]),
        )

        return job_info

    def get_plugin_info(self):
        take: c4d.modules.takesystem.BaseTake = (
            self._instance.data["transientData"]["take"]
        )
        plugin_info = Cinema4DPluginInfo(
            SceneFile=self.scene_path,
            Version=c4d.GetC4DVersion(),
            Take=take.GetName(),
        )
        return asdict(plugin_info)
