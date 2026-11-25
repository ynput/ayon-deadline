import os
from copy import deepcopy
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
from ayon_deadline.lib import MAX_CHUNK_SIZE


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
    RenderPass: str = field(default="")
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
    SlapCompSources: str = field(default="")
    AutoTile: bool = field(default=False)
    TilesX: int = field(default=0)
    TilesY: int = field(default=0)
    AutoTileRes: str = field(default="")
    TileIndex: int = field(default=-1)
    TileSuffix: str = field(default="")
    Stitch: bool = field(default=False)


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
                maximum=MAX_CHUNK_SIZE
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

    def get_job_info(
        self,
        job_info=None,
        dependency_job_ids=None,
        use_dcc_plugin=True
    ):
        """Houdini specific get_job_info with extra kwargs.

        Arguments:
            job_info (None | PublishDeadlineJobInfo): dataclass
                 object with collected values from Settings and
                 Publisher UI.
            dependency_job_ids (None | list[str]): Job ids that should
                become input dependencies to this submission.
            use_dcc_plugin (bool): Whether to submit using Deadline Houdini
                plugin or using the renderer-specific standalone render plugin
                like Husk, Arnold, etc.
        """
        instance = self._instance
        context = instance.context

        # Whether Deadline render submission is being split in two
        # (extract + render)
        split_render_job = instance.data.get("splitRender")

        job_type = "[RENDER]"
        if split_render_job and not use_dcc_plugin:
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
        if split_render_job and not use_dcc_plugin:
            job_info.IsFrameDependent = bool(instance.data.get(
                "splitRenderFrameDependent", True))

        attribute_values = self.get_attr_values_from_data(instance.data)
        if split_render_job and use_dcc_plugin:
            job_info.Priority = attribute_values.get(
                "export_priority", self.export_priority
            )
            job_info.ChunkSize = attribute_values.get(
                "export_chunk", self.export_chunk_size
            )
            # A render that is not frame dependent exports a single file
            #  (e.g. a USD file without $F). Export it in one task, since
            #  multiple machines cannot write to the same file.
            if not instance.data.get("splitRenderFrameDependent", True):
                job_info.ChunkSize = MAX_CHUNK_SIZE
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

        super().process(instance)

        # TODO: Avoid the need for this logic here, needed for submit publish
        # Store output dir for unified publisher (filesequence)
        output_dir = os.path.dirname(instance.data["files"][0])
        instance.data["outputDir"] = output_dir

    def _get_render_settings(self, instance, rop_node) -> str:
        render_settings_prim = instance.data.get("rendersettings")

        if render_settings_prim is not None:
            render_settings = render_settings_prim.GetPath().pathString
        else:
            # Backwards compatibility for AYON houdini <= 0.10.8
            render_settings = (
                rop_node.evalParm("rendersettings")
                or instance.data["stage"].GetMetadata("renderSettingsPrimPath")
                or "/Render/rendersettings"
            )
        return render_settings

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

        render_pass = instance.data.get("renderpass")
        render_pass_path = render_pass.GetPath().pathString if render_pass else ""  # noqa
        render_settings = self._get_render_settings(instance, rop_node)

        # Get SlapComps
        # Instance data comes from `CollectSlapComps` plugin in Houdini addon.
        slapcomps: "list[str]" = instance.data.get("slapComp", [])

        # Like the ROP, pass nothing when Tiled Render is off. With Render
        # All Tiles and Stitch Final Image (Houdini 21+) husk renders and
        # stitches every tile in one task; otherwise each tile gets its own
        # job, see `HoudiniSubmitDeadlineUsdRender._submit_split_render_jobs`
        tiled = rop_node.evalParm("husk_tile")
        auto_tile = bool(
            tiled
            and rop_node.parm("husk_autotile")
            and rop_node.evalParm("husk_autotile")
        )
        tiles_x = tiles_y = 0
        auto_tile_res = tile_suffix = ""
        if tiled:
            tiles_x, tiles_y = rop_node.evalParmTuple("husk_tilecount")
            if auto_tile:
                auto_tile_res = "{} {}".format(
                    *rop_node.evalParmTuple("husk_tilemaxres"))
            else:
                # husk expands $F or %d in the suffix to the tile index.
                tile_suffix = (
                    rop_node.parm("husk_tilesuffix").unexpandedString())

        return HuskStandalonePluginInfo(
            SceneFile=instance.data["ifdFile"],
            Renderer=rop_node.evalParm("renderer"),
            RenderSettings=render_settings,
            RenderPass=render_pass_path,
            Purpose=rop_node.evalParm("husk_purpose"),
            Complexity=rop_node.evalParm("husk_complexity"),
            Snapshot=snapshot_interval,
            PreRender=rop_node.evalParm("husk_prerender"),
            PreFrame=rop_node.evalParm("husk_preframe"),
            PostFrame=rop_node.evalParm("husk_postframe"),
            PostRender=rop_node.evalParm("husk_postrender"),
            RestartDelegate=restart_delegate,
            Version=hou_major_minor,
            SlapCompSources="\n".join(slapcomps),
            AutoTile=auto_tile,
            TilesX=tiles_x,
            TilesY=tiles_y,
            AutoTileRes=auto_tile_res,
            TileSuffix=tile_suffix,
        )

    def _get_families(self, instance: pyblish.api.Instance) -> "set[str]":
        product_base_type = instance.data.get("productBaseType")
        if not product_base_type:
            product_base_type = instance.data["productType"]
        families = set(instance.data.get("families", []))
        families.add(product_base_type)
        return families


class HoudiniSubmitDeadlineUsdRender(HoudiniSubmitDeadline):
    label = "Submit Render to Deadline (USD)"
    families = ["usdrender"]

    def from_published_scene(self, replace_in_path=True):
        # Do not use published workfile paths for USD Render ROP because the
        # Export Job doesn't seem to occur using the published path either, so
        # output paths then do not match the actual rendered paths
        return

    def _submit_split_render_jobs(self, instance, job_id, job_info):
        """Render the tiles when the ROP renders them without stitching them:
        one Deadline tile job for a single frame, else one job per tile. Then
        one job per render product stitches each frame once its tiles are
        rendered.
        """
        plugin_info = self.get_plugin_info(job_type="render")
        tile_count = plugin_info["TilesX"] * plugin_info["TilesY"]
        if plugin_info["AutoTile"] or tile_count < 2:
            super()._submit_split_render_jobs(instance, job_id, job_info)
            return

        auth = instance.data["deadline"]["auth"]
        verify = instance.data["deadline"]["verify"]
        dependency_job_ids = [job_id] if job_id else []
        frame = instance.data["frameStartHandle"]
        single_frame = frame == instance.data["frameEndHandle"]

        if single_frame:
            # A Deadline tile job, one task per tile. Its tasks are tiles, not
            # frames, so it waits for the whole export job.
            tile_job_info = self.get_job_info(
                job_info=deepcopy(job_info),
                dependency_job_ids=dependency_job_ids,
                use_dcc_plugin=False,
            )
            tile_job_info.TileJob = True
            tile_job_info.TileJobFrame = int(frame)
            tile_job_info.TileJobTilesInX = plugin_info["TilesX"]
            tile_job_info.TileJobTilesInY = plugin_info["TilesY"]
            tile_job_info.ChunkSize = 1
            tile_job_info.IsFrameDependent = False
            payload = self.assemble_payload(
                job_info=tile_job_info, plugin_info=plugin_info)
            tile_job_ids = [self.submit(payload, auth, verify)]
        else:
            tile_job_ids = []
            for tile_index in range(tile_count):
                tile_job_info = self.get_job_info(
                    job_info=deepcopy(job_info),
                    dependency_job_ids=dependency_job_ids,
                    use_dcc_plugin=False,
                )
                tile_job_info.Name += " (tile {}/{})".format(
                    tile_index + 1, tile_count)
                payload = self.assemble_payload(
                    job_info=tile_job_info,
                    plugin_info=dict(plugin_info, TileIndex=tile_index),
                )
                tile_job_ids.append(self.submit(payload, auth, verify))
        self.log.info("Tile job ids: %s", tile_job_ids)

        # itilestitch makes one image per call: one stitch job per product.
        stitch_job_ids = []
        for filepath in instance.data["files"]:
            stitch_job_info = self.get_job_info(
                job_info=deepcopy(job_info),
                dependency_job_ids=tile_job_ids,
                use_dcc_plugin=False,
            )
            stitch_job_info.Name = stitch_job_info.Name.replace(
                "[RENDER]", "[STITCH]")
            stitch_job_info.IsFrameDependent = not single_frame
            stitch_job_info.ChunkSize = 1
            stitch_job_info.OutputDirectory.clear()
            stitch_job_info.OutputFilename.clear()
            dirname = os.path.dirname(filepath)
            stitch_job_info.OutputDirectory += dirname.replace("\\", "/")
            stitch_job_info.OutputFilename += os.path.basename(filepath)
            payload = self.assemble_payload(
                job_info=stitch_job_info,
                plugin_info=dict(plugin_info, Stitch=True),
            )
            stitch_job_ids.append(self.submit(payload, auth, verify))
        self.log.info("Stitch job ids: %s", stitch_job_ids)

        # The publish job then waits for the stitch jobs, as for Maya's tile
        # assembly (`ProcessSubmittedJobOnFarm._get_dependency_ids`).
        instance.data["tileRendering"] = True
        instance.data["assemblySubmissionJobs"] = stitch_job_ids
        instance.data["deadline"]["job_info"] = deepcopy(stitch_job_info)
