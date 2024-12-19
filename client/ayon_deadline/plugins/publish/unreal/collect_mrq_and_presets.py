import pyblish.api

import unreal


class CollectMediaRenderQueueAndPresets(pyblish.api.ContextPlugin):
    """Collect Media Render Queue and Presets."""

    order = pyblish.api.CollectorOrder
    label = "Collect Media Render Queue and Presets"
    hosts = ["unreal"]
    families = ["render"]

    def process(self, context):
        self.collect_queue(context)
        self.collect_presets(context)

    def collect_queue(self, context):
        mrq_subsystem = unreal.get_editor_subsystem(
            unreal.MoviePipelineQueueSubsystem
        )
        mrq = mrq_subsystem.get_queue()
        if not mrq:
            raise Exception("No Media Render Queue found")

        if len(mrq.get_jobs()) > 0:
            self.log.info("Emptying current MRQ")
            mrq.delete_all_jobs()

        self.log.info(f"Saving MRQ asset {mrq}")
        context.data["mrq"] = mrq

    def collect_presets(self, context):
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
