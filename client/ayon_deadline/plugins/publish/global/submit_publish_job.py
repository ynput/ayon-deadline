# -*- coding: utf-8 -*-
"""Submit publishing job to farm."""
import os
import json
import re
import getpass
from copy import deepcopy
from typing import List, Any

import clique
import ayon_api
import pyblish.api

from ayon_core.pipeline import publish, Anatomy
from ayon_core.lib.path_templates import TemplateUnsolved

from ayon_core.pipeline.version_start import get_versioning_start
from ayon_core.pipeline.farm.pyblish_functions import (
    create_skeleton_instance,
    create_instances_for_aov,
    attach_instances_to_product,
    prepare_representations,
    create_metadata_path
)

from ayon_deadline import DeadlineAddon
from ayon_deadline.lib import (
    JobType,
    DeadlineJobInfo,
    get_instance_job_envs,
)


def get_resource_files(resources, frame_range=None):
    """Get resource files at given path.

    If `frame_range` is specified those outside will be removed.

    Arguments:
        resources (list): List of resources
        frame_range (list): Frame range to apply override

    Returns:
        list of str: list of collected resources

    """
    res_collections, _ = clique.assemble(resources)
    assert len(res_collections) == 1, "Multiple collections found"
    res_collection = res_collections[0]

    # Remove any frames
    if frame_range is not None:
        for frame in frame_range:
            if frame not in res_collection.indexes:
                continue
            res_collection.indexes.remove(frame)

    return list(res_collection)


class ProcessSubmittedJobOnFarm(pyblish.api.InstancePlugin,
                                publish.AYONPyblishPluginMixin,
                                publish.ColormanagedPyblishPluginMixin):
    """Process Job submitted on farm.

    These jobs are dependent on a deadline job
    submission prior to this plug-in.

    It creates dependent job on farm publishing rendered image sequence.

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

    label = "Submit Image Publishing job to Deadline"
    order = pyblish.api.IntegratorOrder + 0.2
    icon = "tractor"

    targets = ["local"]

    families = ["deadline.submit.publish.job"]
    settings_category = "deadline"

    aov_filter = [
        {
            "name": "maya",
            "value": [r".*([Bb]eauty).*"]
        },
        {
            "name": "blender",
            "value": [r".*([Bb]eauty).*"]
        },
        {
            # for everything from AE
            "name": "aftereffects",
            "value": [r".*"]
        },
        {
            "name": "harmony",
            "value": [r".*"]
        },
        {
            "name": "celaction",
            "value": [r".*"]
        },
        {
            "name": "max",
            "value": [r".*"]
        },
    ]

    # custom deadline attributes
    deadline_department = ""
    deadline_pool = ""
    deadline_group = ""
    deadline_priority = None

    # regex for finding frame number in string
    R_FRAME_NUMBER = re.compile(r'.+\.(?P<frame>[0-9]+)\..+')

    # mapping of instance properties to be transferred to new instance
    #     for every specified family
    instance_transfer = {
        "slate": ["slateFrames", "slate"],
        "review": ["lutPath"],
        "render2d": ["bakingNukeScripts", "version"],
        "renderlayer": ["convertToScanline"]
    }

    # list of family names to transfer to new family if present
    families_transfer = ["render3d", "render2d", "slate"]

    # poor man exclusion
    skip_integration_repre_list = []

    add_rendered_dependencies = False


    def _submit_deadline_post_job(
        self, instance, render_job, instances, rootless_metadata_path
    ):
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
            instances[0]["productName"],
            context,
            instances[0]["productType"],
            override_version
        )

        environment = get_instance_job_envs(instance)
        environment.update(JobType.PUBLISH.get_job_env())

        priority = (
            self.deadline_priority
            or instance.data.get("priority", 50)
        )

        batch_name = self._get_batch_name(instance, render_job)
        username = self._get_username(instance, render_job)
        dependency_ids = self._get_dependency_ids(instance, render_job)

        args = [
            "--headless",
            "publish",
            rootless_metadata_path,
            "--targets", "deadline",
            "--targets", "farm",
        ]
        # TODO remove settings variant handling when not needed anymore
        #   which should be when package.py defines 'core>1.1.1' .
        settings_variant = os.environ["AYON_DEFAULT_SETTINGS_VARIANT"]
        if settings_variant == "staging":
            args.append("--use-staging")
        elif settings_variant != "production":
            args.extend(["--bundle", settings_variant])

        server_name = instance.data["deadline"]["serverName"]
        self.log.debug("Submitting Deadline publish job ...")

        deadline_addon: DeadlineAddon = (
            context.data["ayonAddonsManager"]["deadline"]
        )

        job_info = instance.data["deadline"]["job_info"]
        job_info = DeadlineJobInfo(
            Name=job_name,
            BatchName=batch_name,
            Department=self.deadline_department,
            Priority=priority,
            InitialStatus=job_info.publish_job_state,
            Group=self.deadline_group,
            Pool=self.deadline_pool or None,
            JobDependencies=dependency_ids,
            UserName=username,
            Comment=context.data.get("comment"),
        )
        if output_dir:
            job_info.OutputDirectory.append(output_dir)

        job_info.EnvironmentKeyValue.update(environment)

        if self.add_rendered_dependencies:
            self._add_rendered_dependencies(anatomy, instances, job_info)

        return deadline_addon.submit_ayon_plugin_job(
            server_name,
            args,
            job_info
        )["response"]["_id"]

    def _get_batch_name(self, instance, render_job):
        batch_name = instance.data.get("jobBatchName")
        if not batch_name and render_job:
            batch_name = render_job["Props"]["Batch"]

        if not batch_name:
            batch_name = os.path.splitext(os.path.basename(
                instance.context.data["currentFile"]
            ))[0]
        return batch_name

    def _get_username(self, instance, render_job):
        username = None
        if render_job:
            username = render_job["Props"]["User"]

        if not username:
            username = instance.context.data.get(
                "deadlineUser", getpass.getuser()
            )
        return username

    def _get_dependency_ids(self, instance, render_job):
        # Collect dependent jobs
        if instance.data.get("tileRendering"):
            self.log.info("Adding tile assembly jobs as dependencies...")
            return instance.data.get("assemblySubmissionJobs")

        if instance.data.get("bakingSubmissionJobs"):
            self.log.info(
                "Adding baking submission jobs as dependencies..."
            )
            return instance.data["bakingSubmissionJobs"]

        if render_job and render_job.get("_id"):
            return [render_job["_id"]]
        return None

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

        instance_skeleton_data = create_skeleton_instance(
            instance, families_transfer=self.families_transfer,
            instance_transfer=self.instance_transfer)
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
        do_not_add_review = False
        if instance.data.get("review") is False:
            self.log.debug("Instance has review explicitly disabled.")
            do_not_add_review = True

        aov_filter = {
            item["name"]: item["value"]
            for item in self.aov_filter
        }
        if isinstance(instance.data.get("expectedFiles")[0], dict):
            instances = create_instances_for_aov(
                instance, instance_skeleton_data,
                aov_filter,
                self.skip_integration_repre_list,
                do_not_add_review,
                instance.data["deadline"]["job_info"].Frames
            )
        else:
            representations = prepare_representations(
                instance_skeleton_data,
                instance.data.get("expectedFiles"),
                anatomy,
                aov_filter,
                self.skip_integration_repre_list,
                do_not_add_review,
                instance.context,
                self,
                instance.data["deadline"]["job_info"].Frames
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

        render_job = instance.data.pop("deadlineSubmissionJob", None)
        if not render_job and instance.data.get("tileRendering") is False:
            raise AssertionError(
                "Cannot continue without valid Deadline submission."
            )

        # Transfer the environment from the original job to this dependent
        # job so they use the same environment
        metadata_path, rootless_metadata_path = create_metadata_path(
            instance, anatomy
        )

        deadline_publish_job_id = self._submit_deadline_post_job(
            instance, render_job, instances, rootless_metadata_path
        )

        # Inject deadline url to instances to query DL for job id for overrides
        for inst in instances:
            inst["deadline"] = deepcopy(instance.data["deadline"])
            inst["deadline"].pop("job_info")

        # publish job file
        publish_job = {
            "folderPath": instance_skeleton_data["folderPath"],
            "frameStart": instance_skeleton_data["frameStart"],
            "frameEnd": instance_skeleton_data["frameEnd"],
            "fps": instance_skeleton_data["fps"],
            "source": instance_skeleton_data["source"],
            "user": instance.context.data["user"],
            "intent": instance.context.data.get("intent"),
            "comment": instance.context.data.get("comment"),
            "job": render_job or {},
            "instances": instances
        }

        # Note that a version of 0 is a valid version number,
        # so we explicitly check for `None` value
        # instance override version
        collected_version = instance.data.get("version")
        if collected_version is None:
            # workfile version
            collected_version = instance.context.data.get("version")
        if collected_version is not None:
            publish_job["version"] = collected_version

        if deadline_publish_job_id:
            publish_job["deadline_publish_job_id"] = deadline_publish_job_id

        # add audio to metadata file if available
        audio_file = instance.context.data.get("audioFile")
        if audio_file and os.path.isfile(audio_file):
            publish_job.update({"audio": audio_file})

        self.log.debug(f"Writing metadata json to '{metadata_path}'")
        with open(metadata_path, "w") as f:
            json.dump(publish_job, f, indent=4, sort_keys=True)

    def _get_publish_folder(
        self,
        anatomy,
        template_data,
        folder_entity,
        product_name,
        context,
        product_type,
        version=None
    ):
        """
            Extracted logic to pre-calculate real publish folder, which is
            calculated in IntegrateNew inside of Deadline process.
            This should match logic in:
                'collect_anatomy_instance_data' - to
                    get correct anatomy, family, version for product name and
                'collect_resources_path'
                    get publish_path

        Args:
            anatomy (ayon_core.pipeline.anatomy.Anatomy):
            template_data (dict): pre-calculated collected data for process
            folder_entity (dict[str, Any]): Folder entity.
            product_name (string): Product name (actually group name
                of product)
            product_type (string): for current deadline process it's always
                'render'
                TODO - for generic use family needs to be dynamically
                    calculated like IntegrateNew does
            version (int): override version from instance if exists

        Returns:
            Optional[str]: publish folder where rendered and published files
                will be stored based on 'publish' template

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

        host_name = context.data["hostName"]
        task_info = template_data.get("task") or {}

        template_name = publish.get_publish_template_name(
            project_name,
            host_name,
            product_type,
            task_info.get("name"),
            task_info.get("type"),
        )

        template_data["version"] = version
        template_data["subset"] = product_name
        template_data["family"] = product_type
        template_data["product"] = {
            "name": product_name,
            "type": product_type,
        }

        render_dir_template = anatomy.get_template_item(
            "publish", template_name, "directory"
        )
        try:
            return (
                render_dir_template
                .format_strict(template_data)
                .replace("\\", "/")
            )

        except TemplateUnsolved:
            self.log.error(
                "Publish directory template is unsolved for: "
                f"{template_name} in anatomy. Output directory won't be set."
            )

    def _add_rendered_dependencies(
        self,
        anatomy: Anatomy,
        instances: List[dict[str, Any]],
        job_info: DeadlineJobInfo,
    ) -> None:
        """Adds all expected rendered files as Job dependencies.

        This should help when DL file system is still synchronizing rendered
        files, but publish job starts prematurely.
        """
        for instance in instances:
            for representation in instance["representations"]:
                if isinstance(representation["files"], str):
                    files = [representation["files"]]
                else:
                    files = representation["files"]
                for file_name in files:
                    full_path = os.path.join(
                        representation["stagingDir"], file_name
                    )
                    full_path = anatomy.fill_root(full_path)
                    job_info.AssetDependency += full_path
