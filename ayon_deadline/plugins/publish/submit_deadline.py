import pyblish.api
from ayon_deadline import api as deadline_api

class SubmitDeadline(pyblish.api.InstancePlugin):
    """Submit job to Deadline based on collected info."""

    order = pyblish.api.ExtractorOrder + 0.1
    label = "Submit to Deadline"

    def process(self, instance):
        # Get stored job info
        job_info = instance.data.get("deadlineJobInfo", {})
        if not job_info:
            self.log.debug("No job info collected, using defaults.")
            return

        job_type = job_info.get("JobType", "render")
        # Map job type to Deadline job type
        deadline_job_type = self._map_job_type(job_type)

        # Build JobInfo dictionary for Deadline
        deadline_job_info = {
            "JobType": deadline_job_type,
            "Priority": job_info.get("Priority", 50),
            # Add other fields from profile as needed
        }

        # Submit via Deadline API
        response = deadline_api.submit_job(deadline_job_info, instance.data)
        self.log.info("Job submitted with ID: %s", response)

    @staticmethod
    def _map_job_type(job_type):
        mapping = {
            "render": "Render",
            "publish": "Publish",
            "cache": "Cache"
        }
        return mapping.get(job_type, "Render")
