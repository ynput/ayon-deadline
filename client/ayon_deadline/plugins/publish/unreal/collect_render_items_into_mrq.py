import pyblish.api
import unreal


class CollectRenderItemsIntoMRQ(pyblish.api.InstancePlugin):
    """Collect publishable render items into MRQ."""

    order = pyblish.api.CollectorOrder + 0.1
    label = "Collect Publish Item into MRQ"
    hosts = ["unreal"]
    families = ["render"]

    def process(self, instance):
        ctx = instance.context.data
        self.mrq = ctx["mrq"]  # collector before ensures it's present

        # i'm assuming the mrq is empty at this point
        new_job = self.mrq.allocate_new_job()
        new_job.map = unreal.SoftObjectPath(instance.data["master_level"])
        new_job.sequence = unreal.SoftObjectPath(instance.data["sequence"])
        new_job.set_configuration(instance.context.data["render_presets"][0])
        self.log.debug(f"{new_job = }")
