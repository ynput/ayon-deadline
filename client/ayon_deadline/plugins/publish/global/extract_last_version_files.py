import os

import pyblish.api
import clique

from ayon_core.lib.plugin_tools import fill_sequence_gaps_with_previous
from ayon_core.lib.transcoding import IMAGE_EXTENSIONS
from ayon_core.pipeline import KnownPublishError


class ExtractLastVersionFiles(pyblish.api.InstancePlugin):
    """Copies files of last version to fill gaps.

    This functionality allows to render and replace only selected frames.
    It produces new version with newly rendered frames and rest of them is used
    from last version (if available).
    """

    label = "Copy Last Version Files"
    order = pyblish.api.ExtractorOrder
    families = ["render"]
    targets = ["deadline"]
    settings_category = "deadline"

    def process(self, instance):
        """Process all the nodes in the instance"""
        if not instance.data.get("reuseLastVersion"):
            return

        frame_start = instance.data["frameStart"]
        frame_end = instance.data["frameEnd"]

        for repre in instance.data["representations"]:
            files = repre["files"]

            is_image_sequence = (
                f".{repre['ext']}" in IMAGE_EXTENSIONS and
                isinstance(files, list)
            )
            if not is_image_sequence:
                self.log.debug(
                    f"Representation '{repre['ext']}' is not image sequence"
                )
                continue

            collections = clique.assemble(
                files,
            )[0]
            if len(collections) != 1:
                raise KnownPublishError(
                    "Multiple collections {} found.".format(collections)
                )

            collection = collections[0]

            used_version_entity, last_version_copied_files = (
                fill_sequence_gaps_with_previous(
                    collection=collection,
                    staging_dir=repre["stagingDir"],
                    instance=instance,
                    current_repre_name=repre["name"],
                    start_frame=frame_start,
                    end_frame=frame_end,
                )
            )
            if not last_version_copied_files:
                raise KnownPublishError("Couldn't copy last version files.")

            added_file_names = [
                os.path.basename(file_path)
                for file_path in last_version_copied_files.values()
            ]
            repre["files"].extend(added_file_names)

            # reset representation/instance to original length
            repre["frameStart"] = used_version_entity["attrib"]["frameStart"]
            repre["frameEnd"] = used_version_entity["attrib"]["frameEnd"]
            instance.data.pop("hasExplicitFrames")
