from dataclasses import dataclass, field, asdict

import pyblish.api

from ayon_core.pipeline.publish import AYONPyblishPluginMixin
from ayon_deadline import abstract_submit_deadline


@dataclass
class FusionPluginInfo:
    FlowFile: str = field(default=None)   # Input
    Version: str = field(default=None)    # Mandatory for Deadline

    # Render in high quality
    HighQuality: bool = field(default=True)
    # Whether saver output should be checked after rendering
    # is complete
    CheckOutput: bool = field(default=True)
    # Proxy: higher numbers smaller images for faster test renders
    # 1 = no proxy quality
    Proxy: int = field(default=1)


class FusionSubmitDeadline(abstract_submit_deadline.AbstractSubmitDeadline,
                           AYONPyblishPluginMixin):
    """Submit current Comp to Deadline

    Renders are submitted to a Deadline Web Service as
    supplied via settings key "DEADLINE_REST_URL".

    """
    label = "Submit Fusion to Deadline"
    order = pyblish.api.IntegratorOrder
    hosts = ["fusion"]
    families = ["render", "image"]
    targets = ["local"]
    settings_category = "deadline"

    # presets
    plugin = None

    def process(self, instance):
        if not instance.data.get("farm"):
            self.log.debug("Render on farm is disabled. "
                           "Skipping deadline submission.")
            return

        # TODO: Avoid this hack and instead use a proper way to submit
        #  each render per instance individually
        # TODO: Also, we should support submitting a job per group of instances
        #  that are set to a different frame range. Currently we're always
        #  expecting to render the full frame range for each. Which may mean
        #  we need multiple render jobs but a single publish job dependent on
        #  the multiple separate instance jobs?
        # We are submitting a farm job not per instance - but once per Fusion
        # comp. This is a hack to avoid submitting multiple jobs for each
        # saver separately which would be much slower.
        context = instance.context
        key = "__hasRun{}".format(self.__class__.__name__)
        if context.data.get(key, False):
            return
        else:
            context.data[key] = True

        # Collect all saver instances in context that are to be rendered
        saver_instances = []
        context = instance.context
        for inst in context:
            if inst.data["productType"] not in {"image", "render"}:
                # Allow only saver family instances
                continue

            if not inst.data.get("publish", True):
                # Skip inactive instances
                continue

            self.log.debug(inst.data["name"])
            saver_instances.append(inst)

        if not saver_instances:
            raise RuntimeError("No instances found for Deadline submission")

        instance.data["_farmSaverInstances"] = saver_instances

        super().process(instance)

        # Store the response for dependent job submission plug-ins for all
        # the instances
        transfer_keys = ["deadlineSubmissionJob", "deadline"]
        for saver_instance in saver_instances:
            for key in transfer_keys:
                saver_instance.data[key] = instance.data[key]

    def get_job_info(self, job_info=None, **kwargs):
        instance = self._instance

        # Deadline requires integers in frame range
        job_info.Plugin = self.plugin or "Fusion"
        # already collected explicit values for rendered Frames
        if not job_info.Frames:
            job_info.Frames = "{start}-{end}".format(
                start=int(instance.data["frameStartHandle"]),
                end=int(instance.data["frameEndHandle"])
            )

        # We override the default behavior of AbstractSubmitDeadline here to
        # include the output directory and output filename for each individual
        # saver instance, instead of only the current instance, because we're
        # submitting one job for multiple savers
        for saver_instance in instance.data["_farmSaverInstances"]:
            if saver_instance is instance:
                continue

            self._append_job_output_paths(instance, job_info)

        return job_info

    def get_plugin_info(self):
        instance = self._instance
        plugin_info = FusionPluginInfo(
            FlowFile=self.scene_path,
            Version=str(instance.data["app_version"]),
        )
        plugin_payload: dict = asdict(plugin_info)
        return plugin_payload
