from ayon_core.pipeline import publish
import unreal

class ExtractMRQAsManifest(publish.Extractor):
    label = "Extract Media Render Queue as Manifest"
    hosts = ["unreal"]
    families = ["render", "render.farm"]

    def process(self, instance):
        self.mrq = instance.data["mrq"]
