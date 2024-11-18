import os
import copy
from dataclasses import dataclass, field, asdict

from ayon_core.pipeline import (
    AYONPyblishPluginMixin
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
        from ayon_max.api.lib_rendersettings import RenderSettings

        instance = self._instance
        job_info = copy.deepcopy(self.job_info)
        plugin_info = copy.deepcopy(self.plugin_info)
        plugin_data = {}

        multipass = get_multipass_setting(project_settings)
        if multipass:
            plugin_data["DisableMultipass"] = 0
        else:
            plugin_data["DisableMultipass"] = 1

        files = instance.data.get("expectedFiles")
        if not files:
            raise KnownPublishError("No render elements found")
        first_file = next(self._iter_expected_files(files))
        old_output_dir = os.path.dirname(first_file)
        output_beauty = RenderSettings().get_render_output(instance.name,
                                                           old_output_dir)
        rgb_bname = os.path.basename(output_beauty)
        dir = os.path.dirname(first_file)
        beauty_name = f"{dir}/{rgb_bname}"
        beauty_name = beauty_name.replace("\\", "/")
        plugin_data["RenderOutput"] = beauty_name
        # as 3dsmax has version with different languages
        plugin_data["Language"] = "ENU"

        renderer_class = get_current_renderer()

        renderer = str(renderer_class).split(":")[0]
        if renderer in [
            "ART_Renderer",
            "Redshift_Renderer",
            "V_Ray_6_Hotfix_3",
            "V_Ray_GPU_6_Hotfix_3",
            "Default_Scanline_Renderer",
            "Quicksilver_Hardware_Renderer",
        ]:
            render_elem_list = RenderSettings().get_render_element()
            for i, element in enumerate(render_elem_list):
                elem_bname = os.path.basename(element)
                new_elem = f"{dir}/{elem_bname}"
                new_elem = new_elem.replace("/", "\\")
                plugin_data["RenderElementOutputFilename%d" % i] = new_elem   # noqa

        if renderer == "Redshift_Renderer":
            plugin_data["redshift_SeparateAovFiles"] = instance.data.get(
                "separateAovFiles")
        if instance.data["cameras"]:
            camera = instance.data["cameras"][0]
            plugin_info["Camera0"] = camera
            plugin_info["Camera"] = camera
            plugin_info["Camera1"] = camera
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
        rgb_output = RenderSettings().get_batch_render_output(camera)       # noqa
        rgb_bname = os.path.basename(rgb_output)
        dir = os.path.dirname(first_file)
        beauty_name = f"{dir}/{rgb_bname}"
        beauty_name = beauty_name.replace("\\", "/")
        plugin_info["RenderOutput"] = beauty_name
        renderer_class = get_current_renderer()

        renderer = str(renderer_class).split(":")[0]
        if renderer in [
            "ART_Renderer",
            "Redshift_Renderer",
            "V_Ray_6_Hotfix_3",
            "V_Ray_GPU_6_Hotfix_3",
            "Default_Scanline_Renderer",
            "Quicksilver_Hardware_Renderer",
        ]:
            render_elem_list = RenderSettings().get_batch_render_elements(
                instance.name, old_output_dir, camera
            )
            for i, element in enumerate(render_elem_list):
                if camera in element:
                    elem_bname = os.path.basename(element)
                    new_elem = f"{dir}/{elem_bname}"
                    new_elem = new_elem.replace("/", "\\")
                    plugin_info["RenderElementOutputFilename%d" % i] = new_elem   # noqa

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
        plugin_data = {}
        multipass = get_multipass_setting(project_settings)
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

    def from_published_scene(self, replace_in_path=True):
        instance = self._instance
        if instance.data["renderer"] == "Redshift_Renderer":
            self.log.debug("Using Redshift...published scene wont be used..")
            replace_in_path = False
        return replace_with_published_scene_path(
            instance, replace_in_path)

    @staticmethod
    def _iter_expected_files(exp):
        if isinstance(exp[0], dict):
            for _aov, files in exp[0].items():
                for file in files:
                    yield file
        else:
            for file in exp:
                yield file
