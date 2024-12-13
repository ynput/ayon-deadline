import pyblish.api

from unreal import EditorAssetLibrary, MoviePipelinePrimaryConfig


class CollectRenderPresets(pyblish.api.ContextPlugin):
    """Collect Render Preset uassets."""

    order = pyblish.api.CollectorOrder
    label = "Collect Render Preset uassets"
    hosts = ["unreal"]
    families = ["render"]

    def process(self, context):
        all_assets = EditorAssetLibrary.list_assets(
            "/Game",
            recursive=True,
            include_folder=True,
        )
        render_presets = []
        for uasset in all_assets:
            asset_data = EditorAssetLibrary.find_asset_data(uasset)
            _uasset = asset_data.get_asset()
            if not _uasset:
                continue

            if isinstance(_uasset, MoviePipelinePrimaryConfig):
                render_presets.append(_uasset)

        if not render_presets:
            raise Exception("No render presets found in the project")

        self.log.info("Adding the following render presets:")
        for preset in render_presets:
            self.log.info(f" - {preset}")
        context.data["render_presets"] = render_presets
