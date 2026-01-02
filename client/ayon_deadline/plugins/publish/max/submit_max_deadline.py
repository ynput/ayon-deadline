import os
import copy
from dataclasses import dataclass, field, asdict

from ayon_core.pipeline import (
    AYONPyblishPluginMixin,
    tempdir
)
from ayon_core.pipeline.publish.lib import (
    replace_with_published_scene_path
)
from ayon_core.pipeline.publish import KnownPublishError
from ayon_max.api.lib import (
    get_current_renderer,
    get_multipass_setting
)
from ayon_max.api.lib_rendersettings import RenderSettings
from ayon_deadline import abstract_submit_deadline


@dataclass
class MaxPluginInfo(object):
    SceneFile: str = field(default=None)   # Input
    Version: str = field(default=None)  # Mandatory for Deadline
    SaveFile: bool = field(default=True)
    IgnoreInputs: bool = field(default=True)


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

    def process_submission(self):

        instance = self._instance
        filepath = instance.context.data["currentFile"]

        files = instance.data["expectedFiles"]
        if not files:
            raise KnownPublishError("No Render Elements found!")
        first_file = next(self._iter_expected_files(files))
        output_dir = os.path.dirname(first_file)
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
        first_file = next(self._iter_expected_files(files))
        old_output_dir = os.path.dirname(first_file)

        # as 3dsmax has version with different languages
        plugin_data["Language"] = "ENU"
        renderer_class = get_current_renderer()

        renderer = str(renderer_class).split(":")[0]
        plugin_data = self._collect_render_output(
            renderer, old_output_dir, plugin_data
        )
        if renderer == "Redshift_Renderer":
            plugin_data["redshift_SeparateAovFiles"] = instance.data.get(
                "separateAovFiles")

        elif renderer.startswith("V_Ray_"):
            # enable this so that V-Ray frame buffer shows up
            plugin_data["ShowFrameBuffer"] = True

        if instance.data["cameras"]:
            camera = instance.data["cameras"][0]
            plugin_info["Camera0"] = camera
            plugin_info["Camera"] = camera
            plugin_info["Camera1"] = camera

        plugin_info["RenderWidth"] = instance.data.get(
            "resolutionWidth", rt.renderWidth)
        plugin_info["RenderHeight"] = instance.data.get(
            "resolutionHeight", rt.renderHeight)

        published_workfile = os.path.basename(plugin_info["SceneFile"])
        plugin_info["PostLoadScript"] = tmp_pre_load_max_script(
            instance,
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

    def get_plugin_info_through_camera(self, camera):
        """Get the plugin parameters for deadline submission when
        multi-camera is enabled.
        Args:
            infos(dict): a dictionary with plugin info.
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
        first_file = next(self._iter_expected_files(files))
        old_output_dir = os.path.dirname(first_file)
        renderer_class = get_current_renderer()

        renderer = str(renderer_class).split(":")[0]
        plugin_data = self._collect_render_output(
            renderer, old_output_dir, plugin_data
        )

        if camera:
            # set the default camera and target camera
            # (weird parameters from max)
            plugin_data["Camera"] = camera
            plugin_data["Camera1"] = camera
            plugin_data["Camera0"] = None

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
        cameras = instance.data.get("cameras", [])
        renderer = instance.data["renderer"]
        plugin_data = {}
        multipass = get_multipass_setting(renderer, project_settings)
        if multipass:
            plugin_data["DisableMultipass"] = 0
        else:
            plugin_data["DisableMultipass"] = 1
        for cam in cameras:
            job_info = self.get_job_info_through_camera(cam)
            plugin_info = self.get_plugin_info_through_camera(cam)
            plugin_info.update(plugin_data)
            job_info_list.append(job_info)
            plugin_info_list.append(plugin_info)

        return job_info_list, plugin_info_list

    @staticmethod
    def _is_unsupported_renderer_for_published_scene(renderer):
        """Check if renderer doesn't support published scene files."""
        unsupported_renderers = (
            "Redshift_Renderer",
        )
        unsupported_prefixes = (
            "Arnold",
            "V_Ray_",
        )
        return (
            renderer in unsupported_renderers
            or renderer.startswith(unsupported_prefixes)
        )

    @staticmethod
    def _collect_render_output(renderer, dir, plugin_data):
        """Collects render output and render element paths based on
        renderer type.
        Args:
            renderer (str): The name of the current renderer.
            dir (str): The directory where render outputs should be saved.
            plugin_data (dict): The dictionary to populate with output paths.
        Returns:
            dict: Updated plugin_data with render output paths.

        """
        from pymxs import runtime as rt
        from ayon_max.api.lib_rendersettings import is_supported_renderer
        # Handle render elements
        if is_supported_renderer(renderer):
            render_elem_list = RenderSettings().get_render_element()
            for i, element in enumerate(render_elem_list):
                elem_bname = os.path.basename(element)
                new_elem_path = os.path.join(dir, elem_bname)
                plugin_data[f"RenderElementOutputFilename{i}"] = new_elem_path

        # Handle main render output
        if renderer.startswith("V_Ray_"):
            plugin_data["RenderOutput"] = ""
        else:
            render_output = rt.rendOutputFilename
            plugin_data["RenderOutput"] = render_output.replace("\\", "/")

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


def tmp_pre_load_max_script(instance, original_workfile, publish_workfile):
    """Temporary function to provide pre-load maxscript for deadline
    submission. This is a workaround for Deadline issue where it
    doesn't load the scene properly before rendering.

    Returns:
        str: Maxscript code as a string.
    """
    temp_dir = tempdir.get_temp_dir(
        instance.context.data["projectName"],
        use_local_temp=True)

    max_script = f"""
fn PublishWorkileRenderOutput =
(
    rendererName = renderers.production as string
    original_workfile = "{original_workfile}"
    publish_workfile = "{publish_workfile}"
    if matchPattern rendererName pattern:"V_Ray*" then
    (
        if matchPattern rendererName pattern:"*GPU*" then
        (
            original_filename = renderers.production.V_Ray_settings.output_rawfilename
            new_filename = substituteString original_filename original_workfile publish_workfile
            renderers.production.V_Ray_settings.output_rawfilename = new_filename
            if renderers.production.V_Ray_settings.output_splitgbuffer do (
                original_Aovfilename = renderers.production.V_Ray_settings.output_splitfilename
                new_aovfilename = substituteString original_filename original_workfile publish_workfile
                renderers.production.V_Ray_settings.output_splitfilename = new_aovfilename
            )
        )
        else
        (
            original_filename = renderers.production.output_rawfilename
            new_filename = substituteString original_filename original_workfile publish_workfile
            renderers.production.output_rawfilename = new_filename
            if renderers.production.output_splitgbuffer do (
                original_Aovfilename = renderers.production.output_splitfilename
                new_aovfilename = substituteString original_filename original_workfile publish_workfile
                renderers.production.output_splitfilename = new_aovfilename
            )
        )
    )
    else
    (
        original_filename = renderOutput
        new_filename = substituteString original_filename original_workfile publish_workfile
        renderOutput = new_filename
        rnMgr = maxOps.GetCurRenderElementMgr()
        for i = 1 to rnMgr.numrenderelements() do
        (
            re = rnMgr.getrenderelement i
            if re.enabled do
            (
                originAovfilename = re.GetRenderElementFileName i
                newAovfilename = substituteString originAovfilename original_workfile publish_workfile
                re.SetRenderElementFileName i newAovfilename
            )
        )
    )
    return true
)
renderOutputPublish = PublishWorkileRenderOutput()

""".format(original_workfile=original_workfile, publish_workfile=publish_workfile)
    script_path =os.path.join(temp_dir, "pre_load_max_script.ms")
    with open(script_path, "w") as script_file:
        script_file.write(max_script)
    print(f"Temporary pre-load maxscript created at: {script_path}")
    return script_path
