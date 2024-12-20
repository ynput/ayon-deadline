from ayon_core.pipeline import publish
from unreal import MoviePipelineEditorLibrary


class ExtractMRQAsManifest(publish.Extractor):
    label = "Extract Media Render Queue as Manifest"
    hosts = ["unreal"]
    families = ["render", "render.farm"]

    def process(self, instance):
        self.anatomy_data = deepcopy(instance.data["anatomyData"])
        self.project_data = deepcopy(instance.data["projectEntity"])

        self.serialize_mrq(instance)

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
