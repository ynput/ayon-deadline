# -*- coding: utf-8 -*-
"""Submit publishing job to farm."""
import os
import json
import re
from copy import deepcopy

import ayon_api
import pyblish.api

from ayon_core.pipeline import publish
from ayon_core.pipeline.version_start import get_versioning_start
from ayon_core.pipeline.farm.pyblish_functions import (
    create_skeleton_instance_cache,
    create_instances_for_cache,
    attach_instances_to_product,
    prepare_cache_representations,
    create_metadata_path
)

from ayon_deadline import DeadlineAddon
from ayon_deadline.lib import (
    JobType,
    DeadlineJobInfo,
    get_instance_job_envs,
)


class ProcessSubmittedCacheJobOnFarm(pyblish.api.InstancePlugin,
                                     publish.AYONPyblishPluginMixin,
                                     publish.ColormanagedPyblishPluginMixin):
    """Process Cache Job submitted on farm
    This is replicated version of submit publish job
    specifically for cache(s).

    These jobs are dependent on a deadline job
    submission prior to this plug-in.

    - In case of Deadline, it creates dependent job on farm publishing
      rendered image sequence.

    Options in instance.data:
        - deadlineSubmissionJob (dict, Required): The returned .json
          data from the job submission to deadline.

        - outputDir (str, Required): The output directory where the metadata
            file should be generated. It's assumed that this will also be
            final folder containing the output files.

        - ext (str, Optional): The extension (including `.`) that is required
            in the output filename to be picked up for image sequence
            publishing.

        - expectedFiles (list or dict): explained below

    """

    label = "Submit cache jobs to Deadline"
    order = pyblish.api.IntegratorOrder + 0.2
    icon = "tractor"
    settings_category = "deadline"

    targets = ["local"]

    hosts = ["houdini"]

    families = ["publish.hou"]

    # custom deadline attributes
    deadline_department = ""
    deadline_pool = ""
    deadline_group = ""
    deadline_priority = None

    # regex for finding frame number in string
    R_FRAME_NUMBER = re.compile(r'.+\.(?P<frame>[0-9]+)\..+')

    def _submit_deadline_post_job(self, instance, job):
        """Submit publish job to Deadline.

        Returns:
            (str): deadline_publish_job_id
        """
        data = instance.data.copy()
        product_name = data["productName"]
        job_name = "Publish - {}".format(product_name)

        context = instance.context
        anatomy = context.data["anatomy"]

        # instance.data.get("productName") != instances[0]["productName"]
        # 'Main' vs 'renderMain'
        override_version = None
        instance_version = instance.data.get("version")  # take this if exists
        if instance_version != 1:
            override_version = instance_version

        output_dir = self._get_publish_folder(
            anatomy,
            deepcopy(instance.data["anatomyData"]),
            instance.data.get("folderEntity"),
            instance.data["productName"],
            context,
            instance.data["productType"],
            override_version
        )

        # Transfer the environment from the original job to this dependent
        # job so they use the same environment
        metadata_path, rootless_metadata_path = \
            create_metadata_path(instance, anatomy)

        environment = get_instance_job_envs(instance)
        environment.update(JobType.PUBLISH.get_job_env())

        priority = self.deadline_priority or instance.data.get("priority", 50)

        args = [
            "--headless",
            'publish',
            rootless_metadata_path,
            "--targets", "deadline",
            "--targets", "farm"
        ]

        dependency_ids = []
        if job.get("_id"):
            dependency_ids.append(job["_id"])

        server_name = instance.data["deadline"]["serverName"]

        self.log.debug("Submitting Deadline publish job ...")
        deadline_addon: DeadlineAddon = (
            context.data["ayonAddonsManager"]["deadline"]
        )

        job_info = instance.data["deadline"]["job_info"]
        job_info = DeadlineJobInfo(
            Name=job_name,
            BatchName=job["Props"]["Batch"],
            Department=self.deadline_department,
            Priority=priority,
            InitialStatus=job_info.publish_job_state,
            Group=self.deadline_group,
            Pool=self.deadline_pool or None,
            JobDependencies=dependency_ids,
            UserName=job["Props"]["User"],
            Comment=context.data.get("comment"),
        )
        job_info.OutputDirectory.append(
            output_dir.replace("\\", "/")
        )
        job_info.EnvironmentKeyValue.update(environment)

        return deadline_addon.submit_ayon_plugin_job(
            server_name, args, job_info
        )["response"]["_id"]

    def process(self, instance):
        # type: (pyblish.api.Instance) -> None
        """Process plugin.

        Detect type of render farm submission and create and post dependent
        job in case of Deadline. It creates json file with metadata needed for
        publishing in directory of render.

        Args:
            instance (pyblish.api.Instance): Instance data.

        """
        if not instance.data.get("farm"):
            self.log.debug("Skipping local instance.")
            return

        anatomy = instance.context.data["anatomy"]

        instance_skeleton_data = create_skeleton_instance_cache(instance)
        """
        if content of `expectedFiles` list are dictionaries, we will handle
        it as list of AOVs, creating instance for every one of them.

        Example:
        --------

        expectedFiles = [
            {
                "beauty": [
                    "foo_v01.0001.exr",
                    "foo_v01.0002.exr"
                ],

                "Z": [
                    "boo_v01.0001.exr",
                    "boo_v01.0002.exr"
                ]
            }
        ]

        This will create instances for `beauty` and `Z` product
        adding those files to their respective representations.

        If we have only list of files, we collect all file sequences.
        More then one doesn't probably make sense, but we'll handle it
        like creating one instance with multiple representations.

        Example:
        --------

        expectedFiles = [
            "foo_v01.0001.exr",
            "foo_v01.0002.exr",
            "xxx_v01.0001.exr",
            "xxx_v01.0002.exr"
        ]

        This will result in one instance with two representations:
        `foo` and `xxx`
        """

        if isinstance(instance.data.get("expectedFiles")[0], dict):
            instances = create_instances_for_cache(
                instance, instance_skeleton_data)
        else:
            representations = prepare_cache_representations(
                instance_skeleton_data,
                instance.data.get("expectedFiles"),
                anatomy
            )

            if "representations" not in instance_skeleton_data.keys():
                instance_skeleton_data["representations"] = []

            # add representation
            instance_skeleton_data["representations"] += representations
            instances = [instance_skeleton_data]

        # attach instances to product
        if instance.data.get("attachTo"):
            instances = attach_instances_to_product(
                instance.data.get("attachTo"), instances
            )

        r''' SUBMiT PUBLiSH JOB 2 D34DLiN3
          ____
        '     '            .---.  .---. .--. .---. .--..--..--..--. .---.
        |     |   --= \   |  .  \/   _|/    \|  .  \  ||  ||   \  |/   _|
        | JOB |   --= /   |  |  ||  __|  ..  |  |  |  |;_ ||  \   ||  __|
        |     |           |____./ \.__|._||_.|___./|_____|||__|\__|\.___|
        ._____.

        '''

        render_job = None
        submission_type = ""
        if instance.data.get("toBeRenderedOn") == "deadline":
            render_job = instance.data.pop("deadlineSubmissionJob", None)
            submission_type = "deadline"

        if not render_job:
            import getpass

            render_job = {}
            self.log.debug("Faking job data ...")
            render_job["Props"] = {}
            # Render job doesn't exist because we do not have prior submission.
            # We still use data from it so lets fake it.
            #
            # Batch name reflect original scene name

            if instance.data.get("assemblySubmissionJobs"):
                render_job["Props"]["Batch"] = instance.data.get(
                    "jobBatchName")
            else:
                batch = os.path.splitext(os.path.basename(
                    instance.context.data.get("currentFile")))[0]
                render_job["Props"]["Batch"] = batch
            # User is deadline user
            render_job["Props"]["User"] = instance.context.data.get(
                "deadlineUser", getpass.getuser())

        deadline_publish_job_id = None
        if submission_type == "deadline":
            self.deadline_url = instance.data["deadline"]["url"]
            assert self.deadline_url, "Requires Deadline Webservice URL"

            deadline_publish_job_id = \
                self._submit_deadline_post_job(instance, render_job)

            # Inject deadline url to instances.
            for inst in instances:
                if "deadline" not in inst:
                    inst["deadline"] = {}
                inst["deadline"] = instance.data["deadline"]
                inst["deadline"].pop("job_info")

        # publish job file
        publish_job = {
            "folderPath": instance_skeleton_data["folderPath"],
            "frameStart": instance_skeleton_data["frameStart"],
            "frameEnd": instance_skeleton_data["frameEnd"],
            "fps": instance_skeleton_data["fps"],
            "source": instance_skeleton_data["source"],
            "user": instance.context.data["user"],
            "version": instance.context.data["version"],  # workfile version
            "intent": instance.context.data.get("intent"),
            "comment": instance.context.data.get("comment"),
            "job": render_job or None,
            "instances": instances
        }

        if deadline_publish_job_id:
            publish_job["deadline_publish_job_id"] = deadline_publish_job_id

        metadata_path, rootless_metadata_path = \
            create_metadata_path(instance, anatomy)

        with open(metadata_path, "w") as f:
            json.dump(publish_job, f, indent=4, sort_keys=True)

    def _get_publish_folder(self, anatomy, template_data,
                            folder_entity, product_name, context,
                            product_type, version=None):
        """
            Extracted logic to pre-calculate real publish folder, which is
            calculated in IntegrateNew inside of Deadline process.
            This should match logic in:
                'collect_anatomy_instance_data' - to
                    get correct anatomy, family, version for product and
                'collect_resources_path'
                    get publish_path

        Args:
            anatomy (ayon_core.pipeline.anatomy.Anatomy):
            template_data (dict): pre-calculated collected data for process
            folder_entity (dict[str, Any]): Folder entity.
            product_name (str): Product name (actually group name of product).
            product_type (str): for current deadline process it's always
                'render'
                TODO - for generic use family needs to be dynamically
                    calculated like IntegrateNew does
            version (int): override version from instance if exists

        Returns:
            (string): publish folder where rendered and published files will
                be stored
                based on 'publish' template
        """

        project_name = context.data["projectName"]
        host_name = context.data["hostName"]
        if not version:
            version_entity = None
            if folder_entity:
                version_entity = ayon_api.get_last_version_by_product_name(
                    project_name,
                    product_name,
                    folder_entity["id"]
                )

            if version_entity:
                version = int(version_entity["version"]) + 1
            else:
                version = get_versioning_start(
                    project_name,
                    host_name,
                    task_name=template_data["task"]["name"],
                    task_type=template_data["task"]["type"],
                    product_type="render",
                    product_name=product_name,
                    project_settings=context.data["project_settings"]
                )

        task_info = template_data.get("task") or {}

        template_name = publish.get_publish_template_name(
            project_name,
            host_name,
            product_type,
            task_info.get("name"),
            task_info.get("type"),
        )

        template_data["subset"] = product_name
        template_data["family"] = product_type
        template_data["version"] = version
        template_data["product"] = {
            "name": product_name,
            "type": product_type,
        }

        render_dir_template = anatomy.get_template_item(
            "publish", template_name, "directory"
        )
        return render_dir_template.format_strict(template_data)
