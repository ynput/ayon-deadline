import pyblish.api
from ayon_core.pipeline import KnownPublishError
from ayon_core.lib import BoolDef, EnumDef, NumberDef, TextDef
from ayon_deadline.lib.deadline_profiles import DeadlineProfilesMixin


class CollectDeadlineJobInfo(pyblish.api.ContextPlugin,
                              DeadlineProfilesMixin):
    """Collect Deadline job info from profiles.

    Applies profile settings to Deadline job info dictionary.
    Supports multiple job types: render, publish, caches.
    """

    order = pyblish.api.CollectorOrder + 0.499
    label = "Collect Deadline Job Info"
    hosts = ["*", "aftereffects", "blender", "celaction", "fusion",
             "harmony", "houdini", "maya", "nuke", "photoshop",
             "resolve", "substancepainter", "tvpaint"]

    # --- Attributes for Publisher UI ---
    # These will be displayed based on the job_type of the profile
    def _get_attr_defs_for_job_type(self, job_type):
        """Return list of attribute definitions for given job_type."""
        attrs = []
        if job_type == "render":
            attrs.extend([
                BoolDef("render_suspend_job", default=False,
                        label="Suspend job"),
                NumberDef("render_priority", default=50,
                          label="Priority"),
                TextDef("render_pool", default="",
                        label="Pool"),
                TextDef("render_pool_secondary", default="",
                        label="Secondary Pool"),
                NumberDef("render_chunk_size", default=1,
                          label="Chunk Size"),
                NumberDef("render_concurrent_tasks", default=1,
                          label="Concurrent Tasks"),
                BoolDef("render_force_render", default=False,
                        label="Force Render"),
            ])
        elif job_type == "publish":
            attrs.extend([
                BoolDef("publish_suspend_job", default=False,
                        label="Suspend job"),
                NumberDef("publish_priority", default=50,
                          label="Priority"),
                TextDef("publish_pool", default="",
                        label="Pool"),
                TextDef("publish_pool_secondary", default="",
                        label="Secondary Pool"),
            ])
        elif job_type == "caches":
            attrs.extend([
                BoolDef("caches_suspend_job", default=False,
                        label="Suspend job"),
                NumberDef("caches_priority", default=50,
                          label="Priority"),
                TextDef("caches_pool", default="",
                        label="Pool"),
                TextDef("caches_pool_secondary", default="",
                        label="Secondary Pool"),
                NumberDef("caches_chunk_size", default=1,
                          label="Chunk Size"),
            ])
        return attrs

    def process(self, context):
        # Ensure the context has a 'deadlineJobInfo' dict
        if "deadlineJobInfo" not in context.data:
            context.data["deadlineJobInfo"] = {}

        # Determine job type from context
        # (In practice, this would be set by the submit plugin)
        job_type = context.data.get("deadlineJobType", "render")

        # Collect profiles matching the job type
        project_name = context.data["projectName"]
        task_entity = context.data.get("taskEntity", {})
        folder_path = context.data.get("folderPath", "")
        task_name = context.data.get("taskName", "")
        host_name = context.data.get("hostName", "")

        # Use mixin to get matching settings
        profile = self.find_matching_profile(project_name,
                                             task_entity,
                                             folder_path,
                                             task_name,
                                             host_name,
                                             job_type=job_type)

        if not profile:
            self.log.debug("No matching Deadline Job Info profile found.")
            return

        # Get overridden values from instance attributes
        overrides = {}
        for attr in self._get_attr_defs_for_job_type(job_type):
            key = attr.key
            if key in context.data:
                overrides[key] = context.data[key]

        # Apply profile settings with overrides
        job_info = context.data["deadlineJobInfo"]
        for key, value in profile.items():
            # Skip job_type key itself
            if key == "job_type":
                continue
            # Use override if exists
            if key in overrides:
                job_info[key] = overrides[key]
            else:
                job_info[key] = value

        self.log.debug("Deadline Job Info updated: %s", job_info)
