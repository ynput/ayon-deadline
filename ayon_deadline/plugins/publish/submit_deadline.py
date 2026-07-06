import pyblish.api
from ayon_deadline import lib


class SubmitDeadline(pyblish.api.InstancePlugin):
    """Submit job to Deadline with collected overrides."""

    order = pyblish.api.IntegratorOrder + 0.1
    label = "Submit to Deadline"

    def process(self, instance):
        job_type = lib.get_job_type_for_family(instance.data["family"])
        if not job_type:
            self.log.debug("No job type for family %s", instance.data["family"])
            return

        context = instance.context
        override_key = f"deadlineJobInfo_{job_type}"
        overrides = context.data.get(override_key, {})

        # Merge with instance-level overrides if any
        instance_overrides = instance.data.get("deadlineJobInfo", {})
        if instance_overrides:
            overrides.update(instance_overrides)

        if not overrides:
            return

        # Apply overrides to job info
        job_info = instance.data.get("jobInfo", {})
        if job_info is None:
            job_info = {}
        job_info.update(overrides)
        instance.data["jobInfo"] = job_info

        self.log.debug("Applied job info overrides for %s: %s", job_type, overrides)
