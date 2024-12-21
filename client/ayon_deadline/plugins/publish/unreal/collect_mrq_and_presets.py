import pyblish.api

import unreal


class CollectMediaRenderPresets(pyblish.api.ContextPlugin):
    """Collect Media Render Presets."""

    order = pyblish.api.CollectorOrder
    label = "Collect Media Render Presets"
    hosts = ["unreal"]
    families = ["render"]

    def process(self, context):
        all_assets = unreal.EditorAssetLibrary.list_assets(
            "/Game",
            recursive=True,
            include_folder=True,
        )
        render_presets = []
        for uasset in all_assets:
            asset_data = unreal.EditorAssetLibrary.find_asset_data(uasset)
            _uasset = asset_data.get_asset()
            if not _uasset:
                continue

            if isinstance(_uasset, unreal.MoviePipelinePrimaryConfig):
                render_presets.append(_uasset)

        if not render_presets:
            raise Exception("No render presets found in the project")

        self.log.info("Adding the following render presets:")
        for preset in render_presets:
            self.log.info(f" - {preset}")
        context.data["render_presets"] = render_presets
