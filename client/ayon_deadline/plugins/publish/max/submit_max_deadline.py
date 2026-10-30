import os
import copy
from pathlib import Path
from dataclasses import dataclass, field, asdict

from ayon_core.pipeline import AYONPyblishPluginMixin

from ayon_core.pipeline.publish import KnownPublishError
from ayon_max.api.lib import (
    get_current_renderer,
    get_multipass_setting
)
from ayon_deadline import abstract_submit_deadline


@dataclass
class MaxPluginInfo(object):
    SceneFile: str = field(default=None)   # Input
    Version: str = field(default=None)  # Mandatory for Deadline
    SaveFile: bool = field(default=True)
    IgnoreInputs: bool = field(default=True)


def frames_to_deadline_string(frames) -> str:
    """Convert frame numbers to a Deadline frame list.

    Deadline accepts a comma separated list of frames and frame ranges, e.g.
    ``"1001,1005-1007"``.

    Args:
        frames (Iterable[int]): Frames which are rendered.

    Returns:
        str: Frame list for the Deadline ``JobInfo.Frames`` field.

    """
    frames = sorted({int(frame) for frame in frames})
    if not frames:
        return ""
    ranges = []
    start = previous = frames[0]
    for frame in frames[1:]:
        if frame == previous + 1:
            previous = frame
            continue
        ranges.append(
            f"{start}-{previous}" if start != previous else str(start)
        )
        start = previous = frame
    ranges.append(f"{start}-{previous}" if start != previous else str(start))
    return ",".join(ranges)


class MaxSubmitDeadline(abstract_submit_deadline.AbstractSubmitDeadline,
                        AYONPyblishPluginMixin):

    label = "Submit Render to Deadline"
    hosts = ["max"]
    families = ["maxrender"]
    targets = ["local"]
    settings_category = "deadline"

    def get_job_info(self, job_info=None):

        instance = self._instance
        job_info.Plugin = instance.data.get("plugin") or "3dsmax"

        job_info.EnableAutoTimeout = True
        # A frame fix renders only the frames to fix, the remaining frames are
        # copied from the previous version by the farm publish job. The
        # restriction is applied here, on the copy of the job info, because the
        # publish metadata has to keep the full frame range (otherwise the
        # publish job builds representations only for the rendered frames and
        # the copy extractor has no target frames).
        frames_to_render = instance.data.get("frames_to_render")
        if frames_to_render and not job_info.Frames:
            job_info.Frames = frames_to_deadline_string(frames_to_render)
            self.log.info(
                "Rendering only the frames to fix: %s", job_info.Frames
            )
        # already collected explicit values for rendered Frames
        elif not job_info.Frames:
            # Deadline requires integers in frame range
            frames = "{start}-{end}".format(
                start=int(instance.data["frameStart"]),
                end=int(instance.data["frameEnd"])
            )
            job_info.Frames = frames

        # do not add expected files for multiCamera
        if instance.data.get("multiCamera"):
            job_info.OutputDirectory.clear()
            job_info.OutputFilename.clear()

        return job_info

    def get_plugin_info(self):
        instance = self._instance

        plugin_info = MaxPluginInfo(
            SceneFile=self.scene_path,
            Version=instance.data["maxversion"],
            SaveFile=True,
            IgnoreInputs=True
        )

        plugin_payload = asdict(plugin_info)

        return plugin_payload

    def process(self, instance):
        if not instance.data["farm"]:
            self.log.debug("Render on farm is disabled. "
                           "Skipping deadline submission.")
            return

        # `get_job_info` restricts the frames of the render job to the frames
        # to fix. The abstract submitter stores its job info back into the
        # instance metadata, which is used by `SubmitPublishJob` to build the
        # farm publish representations - the publish job has to keep the
        # original frame range, the remaining frames are copied from the
        # previous version.
        publish_frames = instance.data["deadline"]["job_info"].Frames
        super().process(instance)
        instance.data["deadline"]["job_info"].Frames = publish_frames

    def process_submission(self):

        instance = self._instance
        filepath = instance.context.data["currentFile"]

        files = instance.data["expectedFiles"]
        if not files:
            raise KnownPublishError("No Render Elements found!")
        first_file = next(self._iter_expected_files(files))
        output_dir = Path(first_file).parent.as_posix()
        instance.data["outputDir"] = output_dir

        filename = os.path.basename(filepath)

        payload_data = {
            "filename": filename,
            "dirname": output_dir
        }

        self.log.debug("Submitting 3dsMax render..")
        project_settings = instance.context.data["project_settings"]
        auth = self._instance.data["deadline"]["auth"]
        verify = self._instance.data["deadline"]["verify"]
        if instance.data.get("multiCamera"):
            self.log.debug("Submitting jobs for multiple cameras..")
            payload = self._use_published_name_for_multiples(
                payload_data, project_settings)
            job_infos, plugin_infos = payload
            for job_info, plugin_info in zip(job_infos, plugin_infos):
                self.submit(
                    self.assemble_payload(job_info, plugin_info),
                    auth=auth,
                    verify=verify
                )
        else:
            payload = self._use_published_name(payload_data, project_settings)
            job_info, plugin_info = payload
            self.submit(
                self.assemble_payload(job_info, plugin_info),
                auth=auth,
                verify=verify
            )

    def _use_published_name(self, data, project_settings):
        # Not all hosts can import these modules.
        from ayon_max.api.lib import (
            get_current_renderer,
            get_multipass_setting
        )
        from pymxs import runtime as rt
        instance = self._instance
        # Every farm render has to be submitted with an explicit camera,
        # otherwise the 3ds Max render task falls back to the active viewport
        # (a perspective view) and the result is a wrong camera.
        cameras = instance.data.get("cameras") or []
        if not cameras:
            raise PublishError(
                "No renderable camera found on the instance, the farm render "
                "would fall back to the active viewport. Assign a camera to "
                "the render instance before publishing to the farm."
            )
        job_info = copy.deepcopy(self.job_info)
        plugin_info = copy.deepcopy(self.plugin_info)
        plugin_data = {}
        renderer = instance.data["renderer"]
        multipass = get_multipass_setting(renderer, project_settings)
        if multipass:
            plugin_data["DisableMultipass"] = 0
        else:
            plugin_data["DisableMultipass"] = 1

        files = instance.data.get("expectedFiles")
        if not files:
            raise KnownPublishError("No render elements found")
        first_file = next(self._iter_expected_files(files), None)
        if first_file is None:
            raise KnownPublishError(
                "No expected render files found. When V-Ray renders with "
                "separate render channels the scene needs at least one render "
                "element, otherwise there is nothing to render to."
            )

        # as 3dsmax has version with different languages
        plugin_data["Language"] = "ENU"
        renderer_class = get_current_renderer()

        renderer = str(renderer_class).split(":")[0]
        plugin_data = self._collect_render_output(
            renderer, project_settings, plugin_data
        )
        if renderer == "Redshift_Renderer":
            plugin_data["redshift_SeparateAovFiles"] = instance.data.get(
                "separateAovFiles")

        elif renderer.startswith("V_Ray_"):
            # enable this so that V-Ray frame buffer shows up
            plugin_data["ShowFrameBuffer"] = True

        # The camera was validated at the beginning of the method.
        camera = cameras[0]
        plugin_info["Camera0"] = camera
        plugin_info["Camera"] = camera
        plugin_info["Camera1"] = camera

        plugin_info["RenderWidth"] = instance.data.get(
            "resolutionWidth", rt.renderWidth)
        plugin_info["RenderHeight"] = instance.data.get(
            "resolutionHeight", rt.renderHeight)

        published_workfile = os.path.basename(plugin_info["SceneFile"])
        plugin_info["PostLoadScript"] = self.tmp_pre_load_max_script(
            instance.data["expectedFiles"],
            instance.data["original_workfile_pattern"],
            os.path.splitext(published_workfile)[0],
        )
        self.log.debug("plugin data:{}".format(plugin_data))
        plugin_info.update(plugin_data)

        return job_info, plugin_info

    def get_job_info_through_camera(self, camera):
        """Get the job parameters for deadline submission when
        multi-camera is enabled.
        Args:
            infos(dict): a dictionary with job info.
        """
        instance = self._instance
        context = instance.context
        job_info = copy.deepcopy(self.job_info)
        exp = instance.data.get("expectedFiles")

        src_filepath = context.data["currentFile"]
        src_filename = os.path.basename(src_filepath)
        job_info.Name = "%s - %s - %s" % (
            src_filename, instance.name, camera)
        for filepath in self._iter_expected_files(exp):
            if camera not in filepath:
                continue
            job_info.OutputDirectory += os.path.dirname(filepath)
            job_info.OutputFilename += os.path.basename(filepath)

        return job_info
        # set the output filepath with the relative camera

    def get_plugin_info_through_camera(self, camera, project_settings):
        """Get the plugin parameters for deadline submission when
        multi-camera is enabled.
        Args:
            camera (str): The camera name.
            project_settings (dict): The project settings.
        """
        instance = self._instance
        # set the target camera
        plugin_info = copy.deepcopy(self.plugin_info)

        plugin_data = {}
        # set the output filepath with the relative camera
        if instance.data.get("multiCamera"):
            scene_filepath = instance.context.data["currentFile"]
            scene_filename = os.path.basename(scene_filepath)
            scene_directory = os.path.dirname(scene_filepath)
            current_filename, ext = os.path.splitext(scene_filename)
            camera_name = camera.replace(":", "_")
            camera_scene_name = f"{current_filename}_{camera_name}{ext}"
            camera_scene_filepath = os.path.join(
                scene_directory, f"_{current_filename}", camera_scene_name)
            plugin_data["SceneFile"] = camera_scene_filepath

        files = instance.data.get("expectedFiles")
        if not files:
            raise KnownPublishError("No render elements found")
        first_file = next(self._iter_expected_files(files), None)
        if first_file is None:
            raise KnownPublishError(
                "No expected render files found. When V-Ray renders with "
                "separate render channels the scene needs at least one render "
                "element, otherwise there is nothing to render to."
            )
        renderer_class = get_current_renderer()

        renderer = str(renderer_class).split(":")[0]
        plugin_data = self._collect_render_output(
            renderer, project_settings, plugin_data
        )

        if camera:
            # set the default camera and target camera
            # (weird parameters from max)
            plugin_data["Camera"] = camera

        plugin_info.update(plugin_data)
        return plugin_info

    def _use_published_name_for_multiples(self, data, project_settings):
        """Process the parameters submission for deadline when
            user enables multi-cameras option.
        Args:
            job_info_list (list): A list of multiple job infos
            plugin_info_list (list): A list of multiple plugin infos
        """
        job_info_list = []
        plugin_info_list = []
        instance = self._instance
        cameras = instance.data.get("cameras") or []
        if not cameras:
            raise PublishError(
                "No renderable camera found on the instance, nothing to "
                "submit for multi-camera rendering. Assign at least one "
                "camera to the render instance before publishing to the farm."
            )
        renderer = instance.data["renderer"]
        plugin_data = {}
        multipass = get_multipass_setting(renderer, project_settings)
        if multipass:
            plugin_data["DisableMultipass"] = 0
        else:
            plugin_data["DisableMultipass"] = 1
        for cam in cameras:
            job_info = self.get_job_info_through_camera(cam)
            plugin_info = self.get_plugin_info_through_camera(
                cam, project_settings)
            plugin_info.update(plugin_data)
            job_info_list.append(job_info)
            plugin_info_list.append(plugin_info)

        return job_info_list, plugin_info_list

    def from_published_scene(self, replace_in_path=True):
        """Check and use scene workfile for rendering only when multi-camera
        farm submission is enabled.

        When rendering multi-camera scenes we can't render published workfiles
        because X - so we require the work area workfile to be used to
        support Y.

        Args:
            replace_in_path (bool, optional): Whether to replace the scene path
                with the published scene path. Defaults to True.

        Returns:
            str: Published scene path.
        """
        instance = self._instance
        if instance.data.get("multiCamera"):
            self.log.warning(
                "Use published workfile for rendering "
                "not supported for multi-camera."
            )
            replace_in_path = False

        return super().from_published_scene(replace_in_path=replace_in_path)

    @staticmethod
    def _collect_render_output(renderer, project_settings, plugin_data):
        """Collect the render element output paths for the Deadline plugin.

        The paths come from `RenderProducts` so the submission reuses the
        naming the publish expects for the render elements (V-Ray split
        filename, Redshift/Scanline element filenames, ...). Entries without a
        filename are skipped: an undefined filename makes the 3ds Max render
        task fail with "Unable to convert: undefined to type: String".

        Args:
            renderer (str): The name of the current renderer.
            project_settings (dict): The project settings.
            plugin_data (dict): The dictionary to populate with output paths.

        Returns:
            dict: Updated plugin_data with render output paths.

        """
        from ayon_max.api.lib_renderproducts import RenderProducts
        from pymxs import runtime as rt

        products = RenderProducts(project_settings)
        image_format = products.image_format()
        # Build the expected files for a reference frame and turn the trailing
        # frame number into the Deadline frame padding token. Only the trailing
        # token is replaced, a "0000" elsewhere in the path (version folder,
        # element name, ...) is left untouched.
        reference_frame = 0
        frame_token = f"{reference_frame:04d}"
        expected_by_element = {}
        for element_name, filepath in (
                products.get_render_element_and_filepath(image_format)):
            if not element_name or not filepath:
                continue
            expected_files = products.get_expected_files(
                filepath, [reference_frame], element_name, renderer)
            if not expected_files:
                continue
            expected_path = Path(expected_files[0])
            frame_suffix = f"{frame_token}{expected_path.suffix}"
            if not expected_path.name.endswith(frame_suffix):
                continue
            pattern = (
                f"{expected_path.name[:-len(frame_suffix)]}"
                f"####{expected_path.suffix}"
            )
            expected_by_element[element_name] = (
                expected_path.parent / pattern).as_posix()

        # Only pass entries for render elements that exist in the scene, the
        # Deadline plugin maps them by their render element index.
        element_mgr = rt.maxOps.GetCurRenderElementMgr()
        for index in range(element_mgr.NumRenderElements()):
            element = element_mgr.GetRenderElement(index)
            if not element:
                continue
            element_name = str(element).split(":")[-1]
            filename = expected_by_element.get(element_name)
            if filename:
                plugin_data[f"RenderElementOutputFilename{index}"] = filename

        return plugin_data

    @staticmethod
    def _iter_expected_files(exp):
        if isinstance(exp[0], dict):
            for _aov, files in exp[0].items():
                for file in files:
                    yield file
        else:
            for file in exp:
                yield file

    def tmp_pre_load_max_script(
            self,
            expected_files: dict,
            original_workfile: str,
            publish_workfile: str
        ) -> str:
        """Temporary function to provide pre-load maxscript for deadline
        submission. This is a workaround for Deadline issue where it
        doesn't load the scene properly before rendering.

        Args:
            expected_files (dict): A dictionary of expected rendered files.
            original_workfile (str): The original workfile name pattern.
            publish_workfile (str): The published workfile name pattern.

        Returns:
            str: Maxscript code as a string.
        """

        max_script = f"""
    fn PublishWorkfileRenderOutput =
    (
        rendererName = renderers.production as string
        original_workfile = "{original_workfile}"
        publish_workfile = "{publish_workfile}"

        if matchPattern rendererName pattern:"V_Ray*" then
        (
            if matchPattern rendererName pattern:"*GPU*" then
            (
                if renderers.production.V_Ray_settings.output_saverawfile do
                (
                    if renderers.production.V_Ray_settings.output_rawfilename != undefined do
                    (
                        original_filename = renderers.production.V_Ray_settings.output_rawfilename
                        new_filename = substituteString original_filename original_workfile publish_workfile
                        renderers.production.V_Ray_settings.output_rawfilename = new_filename
                    )
                )

                if renderers.production.V_Ray_settings.output_splitgbuffer do
                (
                    if renderers.production.V_Ray_settings.output_splitfilename != undefined do
                    (
                        original_Aovfilename = renderers.production.V_Ray_settings.output_splitfilename
                        new_aovfilename = substituteString original_Aovfilename original_workfile publish_workfile
                        renderers.production.V_Ray_settings.output_splitfilename = new_aovfilename
                    )
                )
            )
            else
            (
                if renderers.production.output_saverawfile do
                (
                    if renderers.production.output_rawfilename != undefined do
                    (
                        original_filename = renderers.production.output_rawfilename
                        new_filename = substituteString original_filename original_workfile publish_workfile
                        renderers.production.output_rawfilename = new_filename
                    )
                )

                if renderers.production.output_splitgbuffer do
                (
                    if renderers.production.output_splitfilename != undefined do
                    (
                        original_Aovfilename = renderers.production.output_splitfilename
                        new_aovfilename = substituteString original_Aovfilename original_workfile publish_workfile
                        renderers.production.output_splitfilename = new_aovfilename
                    )
                )
            )
        )
        else if matchPattern rendererName pattern:"Arnold*" then (
            original_filename = rendOutputFilename
            new_filename = substituteString original_filename original_workfile publish_workfile
            rendOutputFilename = new_filename
            aovmgr = renderers.production.AOVManager
            original_arnold_filename = aovmgr.outputPath
            new_arnold_filename = substituteString original_arnold_filename original_workfile publish_workfile
            aovmgr.outputPath = new_arnold_filename

        )
        else
        (
            original_filename = rendOutputFilename
            new_filename = substituteString original_filename original_workfile publish_workfile
            rendOutputFilename = new_filename

            rnMgr = maxOps.GetCurRenderElementMgr()
            if rnMgr != undefined do
            (
                for i = 0 to rnMgr.numrenderelements()-1 do
                (
                    re = rnMgr.getrenderelement i
                    if re.enabled do
                    (
                        originAovfilename = rnMgr.GetRenderElementFileName i
                        if originAovfilename != undefined and originAovfilename != "" do
                        (
                            newAovfilename = substituteString originAovfilename original_workfile publish_workfile
                            rnMgr.SetRenderElementFileName i newAovfilename
                        )
                    )
                )
            )
        )

        return true
    )

    -- Execute the function
    renderOutputPublish = PublishWorkfileRenderOutput()

    """  # noqa: E501
        first_file = next(self._iter_expected_files(expected_files))
        render_dir = Path(os.path.dirname(first_file))
        render_dir.mkdir(parents=True, exist_ok=True)
        script_path = render_dir / "pre_load_max_script.ms"
        script_path = os.path.normpath(script_path.as_posix())

        try:
            with open(script_path, "w") as script_file:
                script_file.write(max_script)
            print(f"Temporary pre-load maxscript created at: {script_path}")
            return script_path

        except Exception as e:
            raise RuntimeError(f"Error creating maxscript file: {str(e)}")
