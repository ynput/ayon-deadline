from ayon_core.pipeline import publish
from unreal import MoviePipelineEditorLibrary


class ExtractMRQAsManifest(publish.Extractor):
    label = "Extract Media Render Queue as Manifest"
    hosts = ["unreal"]
    families = ["render", "render.farm"]

    def process(self, instance):
        self.mrq = instance.data["mrq"]
        _, manifest = (
            MoviePipelineEditorLibrary.save_queue_to_manifest_file(self.mrq)
        )
        manifest_string = (
            MoviePipelineEditorLibrary.convert_manifest_file_to_string(
                manifest
            )
        )
        instance.data["mrq_manifest"] = manifest_string
        self.log.info(f"MRQ Manifest written to: {manifest}")
