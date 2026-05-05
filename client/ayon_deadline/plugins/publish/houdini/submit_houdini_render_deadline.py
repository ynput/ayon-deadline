import os
import re
from copy import deepcopy
from dataclasses import dataclass, field, asdict
from datetime import datetime

import pyblish.api

# Frame-token placeholders Houdini uses in render-product paths
# (e.g. "....1###.exr" or "....$F4.exr"). Replaced with Deadline's
# <STARTFRAME> token for per-task substitution at render time.
_FRAME_TOKEN_RE = re.compile(r"#+|\$F\d*")

from ayon_core.pipeline import AYONPyblishPluginMixin
from ayon_core.lib import (
    is_in_tests,
    TextDef,
    NumberDef,
    get_oiio_tool_args,
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
    SlapCompSources: str = field(default="")
    # Tile rendering. Husk wants `--tile-count X Y` (two values) and a
    # printf-style `--tile-suffix` (e.g. `_tile%02d`); husk substitutes the
    # tile index itself, so the suffix is the same for every tile job and
    # only TileIndex varies. TileIndex=-1 / TilesX=0 / TilesY=0 mean
    # "not a tile job".
    TileIndex: int = field(default=-1)
    TilesX: int = field(default=0)
    TilesY: int = field(default=0)
    TileSuffix: str = field(default="")


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

    def _get_husk_standalone_plugin_info(
        self,
        instance,
        hou_major_minor,
        tile_index=-1,
        tiles_x=0,
        tiles_y=0,
        tile_suffix="",
    ):
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

        # Get SlapComps
        # Instance data comes from `CollectSlapComps` plugin in Houdini addon.
        slapcomps: "list[str]" = instance.data.get("slapComp", [])

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
            Version=hou_major_minor,
            SlapCompSources="\n".join(slapcomps),
            TileIndex=tile_index,
            TilesX=tiles_x,
            TilesY=tiles_y,
            TileSuffix=tile_suffix,
        )

    def _build_tile_assembly_args(
        self, file_patterns, tile_count, tile_suffix_pattern
    ):
        """Build the oiiotool argument string that merges N tile EXRs into
        the original render-product path, for each render product.

        Husk wrote each tile as a cropped multipart OpenEXR — display
        window is the full image, data window is the tile's rectangle,
        pixel data only in that rectangle. The file typically has
        multiple subimages: subimage 0 is RGBA beauty, additional
        subimages are 3-channel AOVs (normals, position, cryptomatte,
        etc.) without alpha.

        We can't use `--over` because the AOV subimages have no alpha
        channel. Instead we use `--add:allsubimages=1`: since the four
        tiles' data windows don't overlap, summing them produces the
        same result as compositing, and `:allsubimages=1` applies the
        op to every subimage in parallel so all AOVs survive.

        Frame placeholder tokens ('####', '$F4', etc.) in the input
        patterns are replaced with Deadline's <STARTFRAME> token so each
        per-task command resolves to a concrete frame number at run time.
        """
        args = []
        for pattern in file_patterns:
            canonical = _FRAME_TOKEN_RE.sub("<STARTFRAME>", pattern)
            base, ext = os.path.splitext(canonical)
            for i in range(tile_count):
                # tile_suffix_pattern is a printf string like "_tile%02d"
                # that husk substitutes with the tile index.
                tile_suffix = tile_suffix_pattern % i
                tile_path = "{}{}{}".format(base, tile_suffix, ext)
                args.append("<QUOTE>{}<QUOTE>".format(tile_path))
                if i > 0:
                    args.append("--add:allsubimages=1")
            args.append("-o")
            args.append("<QUOTE>{}<QUOTE>".format(canonical))
        return " ".join(args)

    def _submit_tile_assembly_job(
        self,
        instance,
        tiles_x,
        tiles_y,
        tile_suffix_pattern,
        depends_on,
        generic_job_info,
        auth,
        verify,
    ):
        """Submit a Deadline CommandLine job that merges per-tile EXRs into
        the final render-product path using oiiotool's --over chain.

        One task per frame (ChunkSize=1, IsFrameDependent=True) so a frame
        can assemble as soon as all its tile-job tasks for that frame are
        done. The whole assembly job depends on every tile render job, so
        nothing starts until the tile fan-out is at least partially through.
        """
        file_patterns = instance.data.get("files") or []
        if not file_patterns:
            self.log.warning(
                "Tile assembly skipped: instance has no 'files' "
                "(render product paths) to merge."
            )
            return None
        if not depends_on:
            self.log.warning(
                "Tile assembly skipped: no tile job ids to depend on."
            )
            return None

        tile_count = tiles_x * tiles_y
        arguments = self._build_tile_assembly_args(
            file_patterns, tile_count, tile_suffix_pattern
        )

        # Build JobInfo. We start from generic_job_info and override the
        # Plugin to CommandLine; get_job_info() would otherwise set it to
        # HuskStandalone for usdrender + use_dcc_plugin=False.
        assembly_job_info = self.get_job_info(
            job_info=deepcopy(generic_job_info),
            dependency_job_ids=depends_on,
            use_dcc_plugin=False,
        )
        assembly_job_info.Plugin = "CommandLine"
        assembly_job_info.Name = "{} (assemble tiles)".format(
            assembly_job_info.Name
        )
        assembly_job_info.IsFrameDependent = True
        assembly_job_info.ChunkSize = 1
        # Output paths for Deadline's "View Output" come from get_job_info()
        # above, which walks instance.data["files"] and appends each path's
        # dirname/basename via the +=-style container that
        # PublishDeadlineJobInfo uses. The canonical render product paths
        # are exactly what the assembled outputs land at, so we don't need
        # to touch OutputDirectory / OutputFilename here.

        # Resolve oiiotool via AYON's canonical helper. It asks the
        # ayon_third_party addon for the bundled binary path first
        # (which is what other AYON tooling uses), falling back to
        # AYON_OIIO_PATHS / system PATH. Returns a list of args; for
        # standard installs that's a single executable path. We send
        # args[0] to Deadline as Executable and prepend any extra
        # args[1:] to Arguments (rare — only if oiiotool is wrapped).
        oiio_args = get_oiio_tool_args("oiiotool")
        if not oiio_args:
            self.log.error(
                "Tile assembly skipped: AYON couldn't resolve an "
                "oiiotool executable. Check that the ayon_third_party "
                "addon is installed and has finished its first-run "
                "binary download, or set AYON_OIIO_PATHS."
            )
            return None
        oiiotool_executable = oiio_args[0]
        if len(oiio_args) > 1:
            arguments = " ".join(oiio_args[1:]) + " " + arguments
        plugin_info = {
            "Executable": oiiotool_executable,
            "Arguments": arguments,
            "Shell": "default",
            "ShellExecute": False,
        }

        payload = self.assemble_payload(
            job_info=assembly_job_info,
            plugin_info=plugin_info,
        )
        assembly_job_id = self.submit(payload, auth, verify)
        self.log.info(
            "Submitted tile assembly job (oiiotool, %s tiles per frame, "
            "%s product(s)) to Deadline: %s",
            tile_count, len(file_patterns), assembly_job_id,
        )
        return assembly_job_id

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

    def process(self, instance):
        if not instance.data.get("farm"):
            self.log.debug("Render on farm is disabled. "
                           "Skipping deadline submission.")
            return

        if not instance.data.get("tileRendering"):
            return super().process(instance)

        # Tile rendering path: submit the export job, then fan out N tile
        # render jobs (each with --tile-index/--tile-count via Husk PluginInfo)
        # depending on the export job. Assembly job is a follow-up — for now
        # tile outputs are produced as separate files with --tile-suffix.
        self._instance = instance
        context = instance.context
        self._deadline_url = instance.data["deadline"]["url"]
        assert self._deadline_url, "Requires Deadline Webservice URL"

        creator_attr = instance.data.get("creator_attributes", {})
        export_skipped = (
            creator_attr.get("render_target") == "local_export_farm_render"
        )

        # Build & submit the export job (mirrors AbstractSubmitDeadline.process)
        generic_job_info = self.get_generic_job_info(instance)
        self.job_info = self.get_job_info(job_info=deepcopy(generic_job_info))

        self._set_scene_path(
            context.data["currentFile"],
            generic_job_info.use_published,
            instance.data.get("stagingDir_is_custom", False),
        )
        if instance.data.get("expectedFiles"):
            self._append_job_output_paths(instance, self.job_info)

        self.plugin_info = self.get_plugin_info()
        self.aux_files = self.get_aux_files()

        plugin_info_data = instance.data["deadline"]["plugin_info_data"]
        if plugin_info_data:
            self.apply_additional_plugin_info(plugin_info_data)

        export_job_id = None
        if not export_skipped:
            export_job_id = self.process_submission()
            self.log.info("Submitted export job to Deadline: %s.", export_job_id)

        instance.data["deadline"]["job_info"] = deepcopy(self.job_info)

        # Fan out tile render jobs. Husk takes a 2D grid via
        # `--tile-count X Y` and a printf-style `--tile-suffix` (it
        # substitutes %d-style tokens with the index itself), so the suffix
        # is the same for every tile job and only TileIndex changes.
        tiles_x = int(instance.data["tilesX"])
        tiles_y = int(instance.data["tilesY"])
        tile_count = tiles_x * tiles_y
        # Husk substitutes %d-style tokens in --tile-suffix with the tile
        # index itself, so we send the same printf pattern to every tile job.
        tile_suffix_pattern = "_tile%02d"
        dependency_job_ids = [export_job_id] if export_job_id else []
        auth = instance.data["deadline"]["auth"]
        verify = instance.data["deadline"]["verify"]

        # Common Houdini version for the husk plugin.
        import hou
        hou_major_minor = hou.applicationVersionString().rsplit(".", 1)[0]

        tile_job_ids = []
        for tile_index in range(tile_count):
            tile_job_info = self.get_job_info(
                job_info=deepcopy(generic_job_info),
                dependency_job_ids=dependency_job_ids,
                use_dcc_plugin=False,
            )
            tile_job_info.Name = "{} (tile {}/{})".format(
                tile_job_info.Name, tile_index + 1, tile_count
            )

            tile_plugin_info = asdict(self._get_husk_standalone_plugin_info(
                instance,
                hou_major_minor,
                tile_index=tile_index,
                tiles_x=tiles_x,
                tiles_y=tiles_y,
                tile_suffix=tile_suffix_pattern,
            ))

            payload = self.assemble_payload(
                job_info=tile_job_info,
                plugin_info=tile_plugin_info,
            )
            tile_job_id = self.submit(payload, auth, verify)
            self.log.info(
                "Submitted tile %s/%s to Deadline: %s",
                tile_index + 1, tile_count, tile_job_id,
            )
            tile_job_ids.append(tile_job_id)

            # Mirror parent's side effect for the last tile job so downstream
            # publish plugins that read this still find a sensible value.
            instance.data["deadline"]["job_info"] = deepcopy(tile_job_info)

        instance.data["tileRenderJobIds"] = tile_job_ids
        self.log.info(
            "Tile rendering submitted: %s tile jobs queued.", tile_count,
        )

        # Tile assembly job. One CommandLine job (Plugin=CommandLine) running
        # oiiotool's --over chain, one task per frame, depending on all tile
        # render jobs (whole-job deps) with IsFrameDependent=True so each
        # merged frame picks up as soon as that frame's tiles complete.
        assembly_job_id = self._submit_tile_assembly_job(
            instance,
            tiles_x=tiles_x,
            tiles_y=tiles_y,
            tile_suffix_pattern=tile_suffix_pattern,
            depends_on=tile_job_ids,
            generic_job_info=generic_job_info,
            auth=auth,
            verify=verify,
        )
        if assembly_job_id:
            instance.data["tileAssemblyJobId"] = assembly_job_id
            # The cross-DCC publish job (ProcessSubmittedJobOnFarm in
            # submit_publish_job.py) reads `assemblySubmissionJobs` when
            # tileRendering=True. Without this, JobDependencies is None
            # and the publish task fires before the renders finish.
            instance.data["assemblySubmissionJobs"] = [assembly_job_id]

        # Maintain the parent's "Store output dir for unified publisher" side
        # effect (mirrors HoudiniSubmitDeadline.process).
        if instance.data.get("files"):
            instance.data["outputDir"] = os.path.dirname(
                instance.data["files"][0]
            )
