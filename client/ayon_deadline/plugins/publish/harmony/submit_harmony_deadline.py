# -*- coding: utf-8 -*-
"""Submitting render job to Deadline."""
import os
from pathlib import Path
from collections import OrderedDict
from zipfile import ZipFile, is_zipfile
import re
import shutil

import attr
import pyblish.api

from ayon_deadline import abstract_submit_deadline


class _ZipFile(ZipFile):
    """Extended check for windows invalid characters."""

    # this is extending default zipfile table for few invalid characters
    # that can come from Mac
    _windows_illegal_characters = ":<>|\"?*\r\n\x00"
    _windows_illegal_name_trans_table = str.maketrans(
        _windows_illegal_characters,
        "_" * len(_windows_illegal_characters)
    )


@attr.s
class PluginInfo(object):
    """Plugin info structure for Harmony Deadline plugin."""

    SceneFile = attr.ib()
    # Harmony version
    Version = attr.ib()

    Camera = attr.ib(default="")
    FieldOfView = attr.ib(default=41.11)
    IsDatabase = attr.ib(default=False)
    ResolutionX = attr.ib(default=1920)
    ResolutionY = attr.ib(default=1080)

    # Resolution name preset, default
    UsingResPreset = attr.ib(default=False)
    ResolutionName = attr.ib(default="HDTV_1080p24")

    PreRenderInlineScript = attr.ib(default=None)

    # --------------------------------------------------
    _outputNode = attr.ib(factory=list)

    @property
    def OutputNode(self):  # noqa: N802
        """Return all output nodes formatted for Deadline.

        Returns:
            dict: as `{'Output0Node', 'Top/renderFarmDefault'}`

        """
        out = {}
        for index, v in enumerate(self._outputNode):
            out["Output{}Node".format(index)] = v
        return out

    @OutputNode.setter
    def OutputNode(self, val):  # noqa: N802
        self._outputNode.append(val)

    # --------------------------------------------------
    _outputType = attr.ib(factory=list)

    @property
    def OutputType(self):  # noqa: N802
        """Return output nodes type formatted for Deadline.

        Returns:
            dict: as `{'Output0Type', 'Image'}`

        """
        out = {}
        for index, v in enumerate(self._outputType):
            out["Output{}Type".format(index)] = v
        return out

    @OutputType.setter
    def OutputType(self, val):  # noqa: N802
        self._outputType.append(val)

    # --------------------------------------------------
    _outputLeadingZero = attr.ib(factory=list)

    @property
    def OutputLeadingZero(self):  # noqa: N802
        """Return output nodes type formatted for Deadline.

        Returns:
            dict: as `{'Output0LeadingZero', '3'}`

        """
        out = {}
        for index, v in enumerate(self._outputLeadingZero):
            out["Output{}LeadingZero".format(index)] = v
        return out

    @OutputLeadingZero.setter
    def OutputLeadingZero(self, val):  # noqa: N802
        self._outputLeadingZero.append(val)

    # --------------------------------------------------
    _outputFormat = attr.ib(factory=list)

    @property
    def OutputFormat(self):  # noqa: N802
        """Return output nodes format formatted for Deadline.

        Returns:
            dict: as `{'Output0Type', 'PNG4'}`

        """
        out = {}
        for index, v in enumerate(self._outputFormat):
            out["Output{}Format".format(index)] = v
        return out

    @OutputFormat.setter
    def OutputFormat(self, val):  # noqa: N802
        self._outputFormat.append(val)

    # --------------------------------------------------
    _outputStartFrame = attr.ib(factory=list)

    @property
    def OutputStartFrame(self):  # noqa: N802
        """Return start frame for output nodes formatted for Deadline.

        Returns:
            dict: as `{'Output0StartFrame', '1'}`

        """
        out = {}
        for index, v in enumerate(self._outputStartFrame):
            out["Output{}StartFrame".format(index)] = v
        return out

    @OutputStartFrame.setter
    def OutputStartFrame(self, val):  # noqa: N802
        self._outputStartFrame.append(val)

    # --------------------------------------------------
    _outputPath = attr.ib(factory=list)

    @property
    def OutputPath(self):  # noqa: N802
        """Return output paths for nodes formatted for Deadline.

        Returns:
            dict: as `{'Output0Path', '/output/path'}`

        """
        out = {}
        for index, v in enumerate(self._outputPath):
            out["Output{}Path".format(index)] = v
        return out

    @OutputPath.setter
    def OutputPath(self, val):  # noqa: N802
        self._outputPath.append(val)

    def set_output(self, node, image_format, output,
                   output_type="Image", zeros=3, start_frame=1):
        """Helper to set output.

        This should be used instead of setting properties individually
        as so index remain consistent.

        Args:
            node (str): harmony write node name
            image_format (str): format of output (PNG4, TIF, ...)
            output (str): output path
            output_type (str, optional): "Image" or "Movie" (not supported).
            zeros (int, optional): Leading zeros (for 0001 = 3)
            start_frame (int, optional): Sequence offset.

        """

        self.OutputNode = node
        self.OutputFormat = image_format
        self.OutputPath = output
        self.OutputType = output_type
        self.OutputLeadingZero = zeros
        self.OutputStartFrame = start_frame

    def serialize(self):
        """Return all data serialized as dictionary.

        Returns:
            OrderedDict: all serialized data.

        """
        def filter_data(a, v):
            if a.name.startswith("_"):
                return False
            if v is None:
                return False
            return True

        serialized = attr.asdict(
            self, dict_factory=OrderedDict, filter=filter_data)
        serialized.update(self.OutputNode)
        serialized.update(self.OutputFormat)
        serialized.update(self.OutputPath)
        serialized.update(self.OutputType)
        serialized.update(self.OutputLeadingZero)
        serialized.update(self.OutputStartFrame)

        return serialized


class HarmonySubmitDeadline(
    abstract_submit_deadline.AbstractSubmitDeadline
):
    """Submit render write of Harmony scene to Deadline.

    Renders are submitted to a Deadline Web Service as
    supplied via the environment variable ``DEADLINE_REST_URL``.

    Harmony workfile is `.zip` file that needs to be unzipped for Deadline
    first (DL expects unzipped folder). In case of use of published workfile,
    `.zip` file is copied to `work/../renders` and unzipped there.

    Note:
        If Deadline configuration is not detected, this plugin will
        be disabled.

    Attributes:
        use_published (bool): Use published scene to render instead of the
            one in work area.

    """

    label = "Submit to Deadline"
    order = pyblish.api.IntegratorOrder + 0.1
    hosts = ["harmony"]
    families = ["render.farm"]
    targets = ["local"]
    settings_category = "deadline"

    def get_job_info(self, job_info=None):
        job_info.Plugin = "HarmonyAYON"
        # already collected explicit values for rendered Frames
        if not job_info.Frames:
            job_info.Frames = "{}-{}".format(
                self._instance.data["frameStartHandle"],
                self._instance.data["frameEndHandle"]
            )

        return job_info

    def _unzip_scene_file(self, published_scene: Path) -> Path:
        """Unzip scene zip file to its directory.

        Unzip scene file (if it is zip file) to work area if dir if available,
        if not to its current directory and
        return path to xstage file there. Xstage file is determined by its
        name.

        Args:
            published_scene (Path): path to zip file.

        Returns:
            Path: The path to unzipped xstage.
        """
        # if not zip, bail out.
        if "zip" not in published_scene.suffix or not is_zipfile(
            published_scene.as_posix()
        ):
            self.log.error("Published scene is not in zip.")
            self.log.error(published_scene)
            raise AssertionError("invalid scene format")

        # TODO eventually replace with registered_host().work_root or similar
        workdir = os.environ.get("AYON_WORKDIR")
        if workdir and os.path.exists(workdir):
            renders_path = os.path.join(workdir, "renders", "harmony")
            os.makedirs(renders_path, exist_ok=True)

            self.log.info(f"Copying '{published_scene}' -> '{renders_path}'")
            shutil.copy(published_scene.as_posix(), renders_path)
            published_scene = Path(
                os.path.join(renders_path), published_scene.name)
            (self._instance.context.data["cleanupFullPaths"].
             append(published_scene))

        xstage_path = (
            published_scene.parent
            / published_scene.stem
            / f"{published_scene.stem}.xstage"
        )
        unzip_dir = (published_scene.parent / published_scene.stem)
        with _ZipFile(published_scene, "r") as zip_ref:
            # UNC path (//?/) added to minimalize risk with extracting
            # to large file paths
            zip_ref.extractall("//?/" + str(unzip_dir.as_posix()))

        # find any xstage files in directory, prefer the one with the same name
        # as directory (plus extension)
        xstage_files = []
        for scene in unzip_dir.iterdir():
            if scene.suffix == ".xstage":
                xstage_files.append(scene)

        # there must be at least one (but maybe not more?) xstage file
        if not xstage_files:
            self.log.error("No xstage files found in zip")
            raise AssertionError("Invalid scene archive")

        ideal_scene = False
        # find the one with the same name as zip. In case there can be more
        # then one xtage file.
        for scene in xstage_files:
            # if /foo/bar/baz.zip == /foo/bar/baz/baz.xstage
            #             ^^^                     ^^^
            if scene.stem == published_scene.stem:
                xstage_path = scene
                ideal_scene = True

        # but sometimes xstage file has different name then zip - in that case
        # use that one.
        if not ideal_scene:
            xstage_path = xstage_files[0]
        return xstage_path

    def get_plugin_info(self):
        # this is path to published scene workfile _ZIP_. Before
        # rendering, we need to unzip it.
        published_scene = Path(
            self.from_published_scene(False))
        self.log.debug(f"Processing {published_scene.as_posix()}")
        xstage_path = self._unzip_scene_file(published_scene)
        render_path = xstage_path.parent / "renders"

        # for submit_publish job to create .json file in
        self._instance.data["outputDir"] = render_path
        # to let DL Explore Output
        # TODO update this when #79 is fixed
        self.job_info.OutputDirectory[0] = render_path.as_posix()
        new_expected_files = []
        render_path_str = str(render_path.as_posix())
        for file in self._instance.data["expectedFiles"]:
            _file = str(Path(file).as_posix())
            expected_dir_str = os.path.dirname(_file)
            new_expected_files.append(
                _file.replace(expected_dir_str, render_path_str)
            )
        audio_file = self._instance.data.get("audioFile")
        if audio_file:
            abs_path = xstage_path.parent / audio_file
            self._instance.context.data["audioFile"] = str(abs_path)

        self._instance.data["source"] = str(published_scene.as_posix())
        self._instance.data["expectedFiles"] = new_expected_files
        harmony_plugin_info = PluginInfo(
            SceneFile=xstage_path.as_posix(),
            Version=(
                self._instance.context.data["harmonyVersion"].split(".")[0]),
            FieldOfView=self._instance.context.data["FOV"],
            ResolutionX=self._instance.data["resolutionWidth"],
            ResolutionY=self._instance.data["resolutionHeight"]
        )

        leading_zeros = str(self._instance.data["leadingZeros"])
        frames_and_ext_pattern = f"0{{{leading_zeros}}}[1-9]\\.[a-zA-Z]{{3}}"
        render_prefix = re.sub(frames_and_ext_pattern, "",
                               self._instance.data["expectedFiles"][0])
        harmony_plugin_info.set_output(
            self._instance.data["setMembers"][0],
            self._instance.data["outputFormat"],
            render_prefix,
            self._instance.data["outputType"],
            self._instance.data["leadingZeros"],
            self._instance.data["outputStartFrame"]
        )

        all_write_nodes = self._instance.context.data["all_write_nodes"]
        disable_nodes = []
        for node in all_write_nodes:
            # disable all other write nodes
            if node != self._instance.data["setMembers"][0]:
                disable_nodes.append("node.setEnable('{}', false)"
                                     .format(node))
        harmony_plugin_info.PreRenderInlineScript = ';'.join(disable_nodes)

        return harmony_plugin_info.serialize()
