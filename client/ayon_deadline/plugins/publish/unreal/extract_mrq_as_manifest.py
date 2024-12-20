import shutil
from copy import deepcopy
from pathlib import Path

from ayon_core.pipeline import Anatomy
from ayon_core.pipeline import publish
from ayon_core.lib import StringTemplate


from unreal import MoviePipelineEditorLibrary


class ExtractMRQAsManifest(publish.Extractor):
    label = "Extract Media Render Queue as Manifest"
    hosts = ["unreal"]
    families = ["render", "render.farm"]

    def process(self, instance):
        self.anatomy_data = deepcopy(instance.data["anatomyData"])
        self.project_data = deepcopy(instance.data["projectEntity"])

        self.serialize_mrq(instance)
        self.copy_manifest_to_publish(instance)

    def serialize_mrq(self, instance):
        # serialize mrq to file and string
        self.mrq = instance.data["mrq"]
        _, manifest = (
            MoviePipelineEditorLibrary.save_queue_to_manifest_file(self.mrq)
        )
        manifest_string = (
            MoviePipelineEditorLibrary.convert_manifest_file_to_string(
                manifest
            )
        )
        instance.data["mrq_manifest"] = (
            manifest_string  # save manifest string for potential submission via string
        )
        self.manifest_to_publish = Path(manifest).resolve()  # save manifest file for potential submission via file

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
        # how can i build a @token?
        _dir_template = project_templates["publish"]["default"][
            "directory"
        ].replace("@version", "version")
        _file_template = project_templates["publish"]["default"][
            "file"
        ].replace("@version", "version")
        dir_template = StringTemplate(_dir_template)
        file_template = StringTemplate(_file_template)
        publish_dir = Path(dir_template.format_strict(template_data))
        publish_file = Path(file_template.format_strict(template_data))
        publish_manifest = publish_dir / publish_file
        self.log.debug(f"{publish_manifest = }")

        if not publish_dir.exists():
            self.log.info(f"Creating publish directory: {publish_dir}")
            publish_dir.mkdir(parents=True)
        self.log.debug(f"{self.manifest_to_publish = }")
        shutil.copyfile(self.manifest_to_publish, publish_manifest)
        instance.data["publish_mrq"] = publish_manifest.as_posix()
