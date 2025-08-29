# -*- coding: utf-8 -*-
"""Collect persistent environment file to be deleted."""
import os
from typing import List, Dict
import pyblish.api


class CollectSceneRenderCleanUpBlender(pyblish.api.InstancePlugin):
    """Collect files and directories to be cleaned up.
    """

    order = pyblish.api.CollectorOrder - 0.1
    label = "Collect Scene Render Clean Up (Blender)"
    targets = ["farm"]

    def process(self, instance):
        representations : List[Dict] = instance.data.get("representations", [])
        self.log.debug(f"representations: {representations}")
        staging_dirs: List[str] = []
        files : List[str] = []
        for repre in representations:
            staging_dir = repre.get("stagingDir")
            blender_tmp_dir = os.path.join(staging_dir, "tmp")
            if not os.path.exists(blender_tmp_dir):
                blender_tmp_dir = os.path.join(
                    os.path.dirname(staging_dir), "tmp")
                if not os.path.exists(blender_tmp_dir):
                    continue
            staging_dirs.append(blender_tmp_dir)
            for tmp_file in os.listdir(blender_tmp_dir):
                files.append(os.path.join(blender_tmp_dir, tmp_file))

        instance.context.data["cleanupFullPaths"].extend(files)
        self.log.debug(f"Files to clean up: {files}")
        instance.context.data["cleanupEmptyDirs"].extend(staging_dirs)
        self.log.debug(f"Staging dirs to clean up: {staging_dirs}")
