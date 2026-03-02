# -*- coding: utf-8 -*-
"""Collect persistent environment file to be deleted."""
import os
from typing import List, Dict
import pyblish.api


class CollectSceneRenderCleanUp(pyblish.api.InstancePlugin):
    """Collect files and directories to be cleaned up
    """

    order = pyblish.api.CollectorOrder - 0.1
    label = "Collect Scene Render Clean Up"
    targets = ["farm"]

    def process(self, instance):
        representations : List[Dict] = instance.data.get("representations", [])
        staging_dirs: List[str] = []
        files : List[str] = []
        for repre in representations:
            staging_dir = repre.get("stagingDir")
            for filename in os.listdir(staging_dir):
                base, _ = os.path.splitext(filename)
                if not base.endswith("_tmp"):
                    continue
                staging_dirs.append(staging_dir)
                files.append(os.path.join(staging_dir, filename))

            # Check for blender temporary dir
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


class CollectTempFileCleanUp(pyblish.api.InstancePlugin):
    """Collect files and directories to be cleaned up"""

    order = pyblish.api.CollectorOrder - 0.2
    label = "Collect Temp File Clean Up"
    targets = ["farm"]

    def process(self, instance):
        env_temp_dir = os.getenv("AYON_TMPDIR")
        if not env_temp_dir:
            return
        for tmp_file in os.listdir(env_temp_dir):
            file_path = os.path.join(env_temp_dir, tmp_file)
            if os.path.isfile(file_path):
                instance.context.data["cleanupFullPaths"].append(file_path)
            elif os.path.isdir(file_path):
                for tmp_script in os.listdir(file_path):
                    script_path = os.path.join(file_path, tmp_script)
                    if os.path.isfile(script_path):
                        instance.context.data["cleanupFullPaths"].append(script_path)
                instance.context.data["cleanupEmptyDirs"].append(file_path)
        self.log.debug(
            "Temp files to clean up: "
            f"{instance.context.data['cleanupEmptyDirs']}"
        )
