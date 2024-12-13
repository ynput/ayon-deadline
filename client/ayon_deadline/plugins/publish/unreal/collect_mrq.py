import pyblish.api
import unreal


class CollectMRQ(pyblish.api.ContextPlugin):
    """Collect MediaRenderQueue uasset."""

    order = pyblish.api.CollectorOrder
    label = "Collect MediaRenderQueue uasset"
    hosts = ["unreal"]
    families = ["render"]

    def process(self, context):
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
