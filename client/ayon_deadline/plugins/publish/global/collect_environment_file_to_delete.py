# -*- coding: utf-8 -*-
"""Collect persistent environment file to be deleted."""
import os

import pyblish.api


class CollectEnvironmentFileToDelete(pyblish.api.ContextPlugin):
    """Marks file with extracted environments to be deleted too.

    'GlobalJobPreLoad' produces persistent environment file which gets created
    only once per OS. This approach limits DB querying, but keeps extracting
    of environments on render workers, not during submission.

    This file is created next to metadata.json and needs to be removed also.
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
            path = anatomy.fill_root(path)
            metadata_folder = os.path.dirname(path)
            for file_name in os.listdir(metadata_folder):
                if file_name.startswith('extractenvironments'):
                    file_path = os.path.join(metadata_folder, file_name)
                    context.data["cleanupFullPaths"].append(file_path)
