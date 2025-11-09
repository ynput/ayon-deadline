import os
from collections.abc import Iterable

import pyblish.api
import clique

from ayon_core.pipeline import PublishValidationError
from ayon_core.lib.transcoding import IMAGE_EXTENSIONS


class ValidateExpectedFiles(pyblish.api.InstancePlugin):
    """Compare rendered and expected files"""

    label = "Validate rendered files from Deadline"
    order = pyblish.api.ValidatorOrder
    families = ["render"]
    targets = ["deadline"]
    settings_category = "deadline"

    # check if actual frame range on render job wasn't different
    # case when artists wants to render only subset of frames
    allow_user_override = True

    def process(self, instance):
        """Process all the nodes in the instance"""
        if instance.data.get("hasExplicitFrames"):
            self.log.debug("Explicit frames rendered, skipping check")
            return

        # get dependency jobs ids for retrieving frame list
        dependent_job_ids = self._get_dependent_job_ids(instance)

        if not dependent_job_ids:
            self.log.warning("No dependent jobs found for instance: {}"
                             "".format(instance))
            return

        # get list of frames from dependent jobs
        frame_list = self._get_dependent_jobs_frames(
            instance, dependent_job_ids)

        for repre in instance.data["representations"]:
            expected_files = self._get_expected_files(repre)

            staging_dir = repre["stagingDir"]
            self.log.debug(f"Validating files in directory: {staging_dir}")
            existing_files = self._get_existing_files(staging_dir)

            is_image = f'.{repre["ext"]}' in IMAGE_EXTENSIONS
            if self.allow_user_override and is_image:
                expected_files = self._recalculate_expected_files(
                    expected_files, frame_list, repre)

            # We don't use set.difference because we do allow other existing
            # files to be in the folder that we might not want to use.
            missing = expected_files - existing_files
            if missing:
                raise RuntimeError(
                    "Missing expected files: {}\n"
                    "Expected files: {}\n"
                    "Existing files: {}".format(
                        sorted(missing),
                        sorted(expected_files),
                        sorted(existing_files)
                    )
                )

    def _recalculate_expected_files(self, expected_files, frame_list, repre):
        # We always check for user override because the user might have
        # also overridden the Job frame list to be longer than the
        # originally submitted frame range
        # todo: We should first check if Job frame range was overridden
        #       at all so we don't unnecessarily override anything
        collection_or_filename = self._get_collection(expected_files)
        job_expected_files = self._get_job_expected_files(
            collection_or_filename, frame_list)

        job_files_diff = job_expected_files.difference(expected_files)
        if job_files_diff:
            self.log.debug(
                "Detected difference in expected output files from "
                "Deadline job. Assuming an updated frame list by the "
                "user. Difference: {}".format(sorted(job_files_diff))
            )

            # Update the representation expected files
            self.log.info("Update range from actual job range "
                          "to frame list: {}".format(frame_list))
            # single item files must be string not list
            repre["files"] = (sorted(job_expected_files)
                              if len(job_expected_files) > 1 else
                              list(job_expected_files)[0])

            # Update the expected files
            expected_files = job_expected_files
        return expected_files

    def _get_dependent_job_ids(self, instance):
        """Returns list of dependent job ids from instance metadata.json

        Args:
            instance (pyblish.api.Instance): pyblish instance

        Returns:
            (list): list of dependent job ids

        """
        dependent_job_ids = []

        # job_id collected from metadata.json
        original_job_id = instance.data["render_job_id"]

        dependent_job_ids_env = os.environ.get("RENDER_JOB_IDS")
        if dependent_job_ids_env:
            dependent_job_ids = dependent_job_ids_env.split(',')
        elif original_job_id:
            dependent_job_ids = [original_job_id]

        return dependent_job_ids

    def _get_dependent_jobs_frames(self, instance, dependent_job_ids):
        """Returns list of frame ranges from all render job.

        Render job might be re-submitted so job_id in metadata.json could be
        invalid. GlobalJobPreload injects current job id to RENDER_JOB_IDS.

        Args:
            instance (pyblish.api.Instance): pyblish instance
            dependent_job_ids (list): list of dependent job ids
        Returns:
            (list)
        """
        all_frame_lists = []

        for job_id in dependent_job_ids:
            job_info = self._get_job_info(instance, job_id)
            frame_list = job_info["Props"].get("Frames")
            if frame_list:
                all_frame_lists.extend(frame_list.split(','))

        return all_frame_lists

    def _get_job_expected_files(self,
                                collection_or_filename,
                                frame_list):
        """Calculates list of names of expected rendered files.

        Might be different from expected files from submission if user
        explicitly and manually changed the frame list on the Deadline job.

        Returns:
            set: Set of expected file names in the staging directory.

        """
        # no frames in file name at all, eg 'renderCompositingMain.withLut.mov'
        # so it is a single file
        if isinstance(collection_or_filename, str):
            return {collection_or_filename}

        # Define all frames from the frame list
        all_frames = set()
        for frames in frame_list:
            if '-' not in frames:  # single frame
                frames = "{}-{}".format(frames, frames)

            start, end = frames.split('-')
            all_frames.update(iter(range(int(start), int(end) + 1)))

        # Return all filename for the collection with the new frames
        collection: clique.Collection = collection_or_filename
        collection.indexes.clear()
        collection.indexes.update(all_frames)
        return set(collection)  # return list of filenames

    def _get_collection(self, files) -> "Iterable[str]":
        """Returns sequence collection or a single filepath.

        Arguments:
            files (Iterable[str]): Filenames to retrieve the collection from.
                If not a sequence detected it will return the single file path.

        Returns:
            clique.Collection | str: Sequence collection or single file path
        """
        # todo: we may need this pattern to stay in sync with the
        #  implementation in `ayon_core.lib.collect_frames`
        # clique.PATTERNS["frames"] supports only `.1001.exr` not `_1001.exr`
        # so we use a customized pattern.
        pattern = "[_.](?P<index>(?P<padding>0*)\\d+)\\.\\D+\\d?$"
        patterns = [pattern]
        collections, remainder = clique.assemble(
            files, minimum_items=1, patterns=patterns)
        if collections:
            return collections[0]
        else:
            # No sequence detected, we assume single frame
            return remainder[0]

    def _get_job_info(self, instance, job_id):
        """Calls DL for actual job info for 'job_id'

        Might be different than job info saved in metadata.json if user
        manually changes job pre/during rendering.

        Args:
            instance (pyblish.api.Instance): pyblish instance
            job_id (str): Deadline job id

        Returns:
            (dict): Job info from Deadline

        """
        server_name = instance.data["deadline"]["serverName"]
        if not server_name:
            raise PublishValidationError(
                "Deadline server name is not filled."
            )

        addons_manager = instance.context.data["ayonAddonsManager"]
        deadline_addon = addons_manager["deadline"]
        return deadline_addon.get_job_info(server_name, job_id)

    def _get_existing_files(self, staging_dir):
        """Returns set of existing file names from 'staging_dir'"""
        existing_files = set()
        for file_name in os.listdir(staging_dir):
            existing_files.add(file_name)
        return existing_files

    def _get_expected_files(self, repre):
        """Returns set of file names in representation['files']

        The representations are collected from `CollectRenderedFiles` using
        the metadata.json file submitted along with the render job.

        Args:
            repre (dict): The representation containing 'files'

        Returns:
            set: Set of expected file_names in the staging directory.

        """
        expected_files = set()

        files = repre["files"]
        if not isinstance(files, list):
            files = [files]

        for file_name in files:
            expected_files.add(file_name)
        return expected_files
