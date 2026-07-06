from ayon_deadline.abstract import DeadlineJobInfo
from ayon_core import style


class CreateDeadlineJob(DeadlineJobInfo):
    """Create Deadline job with job-type-specific fields."""

    def process(self, instance):
        context = instance.context
        job_type = instance.data.get("jobType", "render")
        
        # Build job info from context data with prefix
        prefix = job_type + "_"
        job_info = {}
        for key, value in context.data.items():
            if key.startswith(prefix):
                job_info[key[len(prefix):]] = value
        
        # Merge with instance-specific data
        job_info.update(instance.data.get("deadlineJobInfo", {}))
        
        # Set Deadline job fields
        self.set_job_info(job_info)
        
        # Additional fields
        self.set_plugin_job_info(instance)
        self.set_job_dependencies(instance)

    def set_job_info(self, job_info):
        # Apply job info to instance
        for key, value in job_info.items():
            self.data[key] = value
