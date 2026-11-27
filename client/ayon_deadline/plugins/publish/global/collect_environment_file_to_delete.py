# -*- coding: utf-8 -*-
"""Collect persistent environment file to be deleted."""
import os

import pyblish.api


class CollectEnvironmentFileToDelete(pyblish.api.ContextPlugin):
    """Marks file with extracted environments to be deleted too.

    'GlobalJobPreLoad' produces persistent environment file which gets created
    only once per AYON_SITE_ID which should be set on all (similar) render
    nodes. (More granular values for AYON_SITE_ID could be used if necessary.)

    This approach limits DB querying, but keeps extracting
    of environments on render workers, not during submission.

    These files are created next to metadata.json in `.ayon_env_cache` folder
    and need to be removed also, but only during publish job.
    """

    order = pyblish.api.CollectorOrder
    label = "Collect Environment File"
    targets = ["farm"]

    def process(self, context):
        for instance in context:
            is_persistent = instance.data.get("stagingDir_persistent", False)
            if is_persistent:
                self.log.debug("Staging dir is persistent, no cleaning.")
                return

        publish_data_paths = os.environ.get("AYON_PUBLISH_DATA")
        if not publish_data_paths:
            self.log.warning("Cannot find folder with metadata files.")
            return

        anatomy = context.data["anatomy"]
        paths = publish_data_paths.split(os.pathsep)
        for path in paths:
            if not path:
                continue
            path = anatomy.fill_root(path)
            metadata_folder = os.path.dirname(path)
            shared_env_folder = os.path.join(
                metadata_folder, ".ayon_env_cache")
            if not os.path.exists(shared_env_folder):
                continue
            for file_name in os.listdir(shared_env_folder):
                if file_name.startswith('env_'):
                    file_path = os.path.join(shared_env_folder, file_name)
                    context.data["cleanupFullPaths"].append(file_path)

            context.data["cleanupEmptyDirs"].append(shared_env_folder)
