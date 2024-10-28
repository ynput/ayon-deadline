import json
import pyblish.api

from ayon_deadline.lib import FARM_FAMILIES, JOB_EXTRA_INFO_DATA_KEY
from ayon_applications.utils import get_tools_for_context


class CollectDeadlineJobExtraInfo(pyblish.api.InstancePlugin):
    """Collect set of environment variables to submit with deadline jobs"""
    order = pyblish.api.CollectorOrder + 0.499
    label = "Deadline Farm Extra Info"
    families = FARM_FAMILIES
    targets = ["local"]

    def process(self, instance):

        # Transfer some environment variables from current context
        job_info = instance.data.setdefault(JOB_EXTRA_INFO_DATA_KEY, {})

        # Support Extra Info 0-10 (int key)
        # Project Name, Folder Path, Task Name, App Name
        context = instance.context
        folder_entity = instance.data.get("folderEntity", {})

        # TODO: Make this customizable in settings somehow?
        job_info[0] = folder_entity.get("label") or folder_entity["name"]
        job_info[1] = context.data.get("projectName", "")
        job_info[2] = instance.data.get("folderPath", "")
        job_info[3] = instance.data.get("task", "")
        job_info[4] = instance.data.get("productName", "")
        job_info[5] = instance.context.data.get("appName", "")

        # Supply the tools for the current context so that we can visualize
        # on the farm what the tools were at time of submission
        tools = get_tools_for_context(
            project_name=context.data.get("projectName"),
            folder_entity=context.data.get("folderEntity"),
            task_entity=context.data.get("taskEntity"),
            project_settings=context.data.get("project_settings")
        )
        job_info[6] = " ".join(sorted(tools))

        self.log.debug(
            f"Farm job extra info: {json.dumps(job_info, indent=4)}")