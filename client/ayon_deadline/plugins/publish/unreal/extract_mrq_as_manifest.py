import shutil
from copy import deepcopy
from pathlib import Path

from ayon_core.pipeline import Anatomy
from ayon_core.pipeline import publish
from ayon_core.lib import StringTemplate


import unreal


class ExtractMRQAsManifest(publish.Extractor):
    label = "Extract Media Render Queue as Manifest"
    hosts = ["unreal"]
    families = ["render", "render.farm"]

    def process(self, instance):
        self.anatomy_data = deepcopy(instance.data["anatomyData"])
        self.project_data = deepcopy(instance.data["projectEntity"])

        self.get_work_file_template()
        self.configure_mrq(instance)
        self.serialize_mrq(instance)
        self.copy_manifest_to_publish(instance)


    def get_work_file_template(self):
        # get work file template
        #   how can i build a @token?
        project_templates = self.project_data["config"]["templates"]
        _dir_template = project_templates["publish"]["default"][
            "directory"
        ].replace("@version", "version")
        _file_template = project_templates["publish"]["default"][
            "file"
        ].replace("@version", "version")

        self.dir_template = StringTemplate(_dir_template)
        self.file_template = StringTemplate(_file_template)


    def configure_mrq(self, instance):
        mrq_subsystem = unreal.get_editor_subsystem(
            unreal.MoviePipelineQueueSubsystem
        )
        self.mrq = mrq_subsystem.get_queue()
        for job in self.mrq.get_jobs():
            if job is instance.data["mrq_job"]:
                job.set_is_enabled(True)
                continue
            job.set_is_enabled(False)

    def serialize_mrq(self, instance):
        # serialize mrq to file and string
        self.mrq = instance.data["mrq"]
        _, manifest = (
            unreal.MoviePipelineEditorLibrary.save_queue_to_manifest_file(self.mrq)
        )
        manifest_string = (
            unreal.MoviePipelineEditorLibrary.convert_manifest_file_to_string(
                manifest
            )
        )
        instance.data["mrq_manifest"] = (
            manifest_string  # save manifest string for potential submission via string
        )
        self.manifest_to_publish = Path(manifest).resolve()

    def copy_manifest_to_publish(self, instance):
        # initialize template data
        anatomy = Anatomy(self.project_data["name"])
        template_data = self.anatomy_data
        template_data["root"] = anatomy.roots

        # get current product name and append manifest, set ext to .utxt
        template_data["product"]["name"] += "Manifest"
        template_data["ext"] = "utxt"

        # format the publish path
        project_templates = self.project_data["config"]["templates"]
        template_data["version"] = (
            f"v{template_data['version']:0{project_templates['common']['version_padding']}d}"
        )
        publish_dir = Path(self.dir_template.format_strict(template_data))
        publish_file = Path(self.file_template.format_strict(template_data))
        publish_manifest = publish_dir / publish_file
        self.log.debug(f"{publish_manifest = }")

        if not publish_dir.exists():
            self.log.info(f"Creating publish directory: {publish_dir}")
            publish_dir.mkdir(parents=True)
        self.log.debug(f"{self.manifest_to_publish = }")
        shutil.copyfile(self.manifest_to_publish, publish_manifest)
        instance.data["publish_mrq"] = publish_manifest.as_posix()
