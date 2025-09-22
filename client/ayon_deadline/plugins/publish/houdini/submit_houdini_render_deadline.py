import os
from dataclasses import dataclass, field, asdict
from datetime import datetime

import pyblish.api

from ayon_core.pipeline import AYONPyblishPluginMixin
from ayon_core.lib import (
    is_in_tests,
    TextDef,
    NumberDef
)
from ayon_deadline import abstract_submit_deadline


@dataclass
class DeadlinePluginInfo:
    SceneFile: str = field(default=None)
    OutputDriver: str = field(default=None)
    Version: str = field(default=None)
    IgnoreInputs: bool = field(default=True)


@dataclass
class ArnoldRenderDeadlinePluginInfo:
    InputFile: str = field(default=None)
    Verbose: int = field(default=4)


@dataclass
class MantraRenderDeadlinePluginInfo:
    SceneFile: str = field(default=None)
    Version: str = field(default=None)


@dataclass
class VrayRenderPluginInfo:
    InputFilename: str = field(default=None)
    SeparateFilesPerFrame: bool = field(default=True)


@dataclass
class RedshiftRenderPluginInfo:
    SceneFile: str = field(default=None)
    # Use "1" as the default Redshift version just because it
    # defaults to fallback version in Deadline's Redshift plugin
    # if no version was specified
    Version: str = field(default="1")


@dataclass
class HuskStandalonePluginInfo:
    """Requires Deadline Husk Standalone Plugin.
    See Deadline Plug-in:
        https://github.com/BigRoy/HuskStandaloneSubmitter
    Also see Husk options here:
        https://www.sidefx.com/docs/houdini/ref/utils/husk.html
    """
    SceneFile: str = field()
    # TODO: Below parameters are only supported by custom version of the plugin
    Renderer: str = field(default=None)
    RenderSettings: str = field(default="/Render/rendersettings")
    Purpose: str = field(default="geometry,render")
    Complexity: str = field(default="veryhigh")
    Snapshot: int = field(default=-1)
    LogLevel: str = field(default="2")
    PreRender: str = field(default="")
    PreFrame: str = field(default="")
    PostFrame: str = field(default="")
    PostRender: str = field(default="")
    RestartDelegate: str = field(default="")
    Version: str = field(default="")


class HoudiniSubmitDeadline(
    abstract_submit_deadline.AbstractSubmitDeadline,
    AYONPyblishPluginMixin
):
    """Submit Render ROPs to Deadline.

    Renders are submitted to a Deadline Web Service as
    supplied via the environment variable AVALON_DEADLINE.

    Target "local":
        Even though this does *not* render locally this is seen as
        a 'local' submission as it is the regular way of submitting
        a Houdini render locally.

    """

    label = "Submit Render to Deadline"
    order = pyblish.api.IntegratorOrder
    hosts = ["houdini"]
    families = ["redshift_rop",
                "arnold_rop",
                "mantra_rop",
                "karma_rop",
                "vray_rop"]
    targets = ["local"]
    settings_category = "deadline"

    # presets
    export_priority = 50
    export_chunk_size = 10
    export_group = ""
    export_limits = ""
    export_machine_limit = 0

    @classmethod
    def get_attribute_defs(cls):
        return [
            NumberDef(
                "export_priority",
                label="Export Priority",
                default=cls.export_priority,
                decimals=0
            ),
            NumberDef(
                "export_chunk",
                label="Export Frames Per Task",
                default=cls.export_chunk_size,
                decimals=0,
                minimum=1,
                maximum=1000
            ),
            TextDef(
                "export_group",
                default=cls.export_group,
                label="Export Group Name"
            ),
            TextDef(
                "export_limits",
                default=cls.export_limits,
                label="Export Limit Groups",
                placeholder="value1,value2",
                tooltip="Enter a comma separated list of limit groups."
            ),
            NumberDef(
                "export_machine_limit",
                default=cls.export_machine_limit,
                label="Export Machine Limit",
                tooltip="maximum number of machines for this job."
            ),
        ]

    def get_job_info(self, dependency_job_ids=None, job_info=None):

        instance = self._instance
        context = instance.context

        # Whether Deadline render submission is being split in two
        # (extract + render)
        split_render_job = instance.data.get("splitRender")

        # If there's some dependency job ids we can assume this is a render job
        # and not an export job
        is_export_job = True
        if dependency_job_ids:
            is_export_job = False

        job_type = "[RENDER]"
        if split_render_job and not is_export_job:
            families = self._get_families(instance)
            family_to_render_plugin = {
                "arnold_rop": "Arnold",
                "karma_rop": "Karma",
                "mantra_rop": "Mantra",
                "redshift_rop": "Redshift",
                "usdrender": "HuskStandalone",
                "vray_rop": "Vray",
            }
            for family, render_plugin in family_to_render_plugin.items():
                if family in families:
                    plugin = render_plugin
                    break
            else:
                plugin = "Render"
                self.log.warning(
                    f"No matching render plugin found for families: {families}"
                )
        else:
            plugin = "Houdini"
            if split_render_job:
                export_file = instance.data["ifdFile"]
                extension = os.path.splitext(export_file)[-1] or "SCENE"
                job_type = f"[EXPORT {extension.upper()}]"

        job_info.Plugin = plugin

        filepath = context.data["currentFile"]
        filename = os.path.basename(filepath)
        job_info.Name = "{} - {} {}".format(filename, instance.name, job_type)
        job_info.BatchName = filename

        if is_in_tests():
            job_info.BatchName += datetime.now().strftime("%d%m%Y%H%M%S")

        # already collected explicit values for rendered Frames
        if not job_info.Frames:
            # Deadline requires integers in frame range
            start = instance.data["frameStartHandle"]
            end = instance.data["frameEndHandle"]
            frames = "{start}-{end}x{step}".format(
                start=int(start),
                end=int(end),
                step=int(instance.data["byFrameStep"]),
            )
            job_info.Frames = frames

        # Make sure we make job frame dependent so render tasks pick up a soon
        # as export tasks are done
        if split_render_job and not is_export_job:
            job_info.IsFrameDependent = bool(instance.data.get(
                "splitRenderFrameDependent", True))

        attribute_values = self.get_attr_values_from_data(instance.data)
        if split_render_job and is_export_job:
            job_info.Priority = attribute_values.get(
                "export_priority", self.export_priority
            )
            job_info.ChunkSize = attribute_values.get(
                "export_chunk", self.export_chunk_size
            )
            job_info.Group = attribute_values.get(
                "export_group", self.export_group
            )
            job_info.LimitGroups = attribute_values.get(
                "export_limits", self.export_limits
            )
            job_info.MachineLimit = attribute_values.get(
                "export_machine_limit", self.export_machine_limit
            )

        # TODO change to expectedFiles??
        for i, filepath in enumerate(instance.data["files"]):
            dirname = os.path.dirname(filepath)
            fname = os.path.basename(filepath)
            job_info.OutputDirectory += dirname.replace("\\", "/")
            job_info.OutputFilename += fname

        # Add dependencies if given
        if dependency_job_ids:
            job_info.JobDependencies = dependency_job_ids

        return job_info

    def get_plugin_info(self, job_type=None):
        # Not all hosts can import this module.
        import hou

        instance = self._instance
        context = instance.context

        hou_major_minor = hou.applicationVersionString().rsplit(".", 1)[0]

        # Output driver to render
        if job_type == "render":
            families = self._get_families(instance)
            if "arnold_rop" in families:
                plugin_info = ArnoldRenderDeadlinePluginInfo(
                    InputFile=instance.data["ifdFile"]
                )
            elif "mantra_rop" in families:
                plugin_info = MantraRenderDeadlinePluginInfo(
                    SceneFile=instance.data["ifdFile"],
                    Version=hou_major_minor,
                )
            elif "vray_rop" in families:
                plugin_info = VrayRenderPluginInfo(
                    InputFilename=instance.data["ifdFile"],
                )
            elif "redshift_rop" in families:
                plugin_info = RedshiftRenderPluginInfo(
                    SceneFile=instance.data["ifdFile"]
                )
                # Note: To use different versions of Redshift on Deadline
                #       set the `REDSHIFT_VERSION` env variable in the Tools
                #       settings in the AYON Application plugin. You will also
                #       need to set that version in `Redshift.param` file
                #       of the Redshift Deadline plugin:
                #           [Redshift_Executable_*]
                #           where * is the version number.
                if os.getenv("REDSHIFT_VERSION"):
                    plugin_info.Version = os.getenv("REDSHIFT_VERSION")
                else:
                    self.log.warning((
                        "REDSHIFT_VERSION env variable is not set"
                        " - using version configured in Deadline"
                    ))

            elif "usdrender" in families:
                plugin_info = self._get_husk_standalone_plugin_info(
                    instance, hou_major_minor)

            else:
                self.log.error(
                    "Render Product Type does not support to split render "
                    f"job. Matched product types against: {families}"
                )
                return
        else:
            driver = hou.node(instance.data["instance_node"])
            plugin_info = DeadlinePluginInfo(
                SceneFile=context.data["currentFile"],
                OutputDriver=driver.path(),
                Version=hou_major_minor,
                IgnoreInputs=True
            )

        return asdict(plugin_info)

    def process(self, instance):
        if not instance.data["farm"]:
            self.log.debug("Render on farm is disabled. "
                           "Skipping deadline submission.")
            return

        super(HoudiniSubmitDeadline, self).process(instance)

        # TODO: Avoid the need for this logic here, needed for submit publish
        # Store output dir for unified publisher (filesequence)
        output_dir = os.path.dirname(instance.data["files"][0])
        instance.data["outputDir"] = output_dir

    def _get_husk_standalone_plugin_info(self, instance, hou_major_minor):
        # Not all hosts can import this module.
        import hou

        # Supply additional parameters from the USD Render ROP
        # to the Husk Standalone Render Plug-in
        rop_node = hou.node(instance.data["instance_node"])
        snapshot_interval = -1
        if rop_node.evalParm("dosnapshot"):
            snapshot_interval = rop_node.evalParm("snapshotinterval")

        restart_delegate = 0
        if rop_node.evalParm("husk_restartdelegate"):
            restart_delegate = rop_node.evalParm("husk_restartdelegateframes")

        rendersettings = (
            rop_node.evalParm("rendersettings")
            or "/Render/rendersettings"
        )
        return HuskStandalonePluginInfo(
            SceneFile=instance.data["ifdFile"],
            Renderer=rop_node.evalParm("renderer"),
            RenderSettings=rendersettings,
            Purpose=rop_node.evalParm("husk_purpose"),
            Complexity=rop_node.evalParm("husk_complexity"),
            Snapshot=snapshot_interval,
            PreRender=rop_node.evalParm("husk_prerender"),
            PreFrame=rop_node.evalParm("husk_preframe"),
            PostFrame=rop_node.evalParm("husk_postframe"),
            PostRender=rop_node.evalParm("husk_postrender"),
            RestartDelegate=restart_delegate,
            Version=hou_major_minor
        )

    def _get_families(self, instance: pyblish.api.Instance) -> "set[str]":
        families = set(instance.data.get("families", []))
        families.add(instance.data.get("productType"))
        return families


class HoudiniSubmitDeadlineUsdRender(HoudiniSubmitDeadline):
    label = "Submit Render to Deadline (USD)"
    families = ["usdrender"]

    def from_published_scene(self, replace_in_path=True):
        # Do not use published workfile paths for USD Render ROP because the
        # Export Job doesn't seem to occur using the published path either, so
        # output paths then do not match the actual rendered paths
        return
