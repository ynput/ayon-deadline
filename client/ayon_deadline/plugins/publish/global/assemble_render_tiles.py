import os
import subprocess

import pyblish.api

from ayon_core.lib import get_oiio_tool_args


class AssembleRenderTiles(pyblish.api.InstancePlugin):
    """Assemble per-tile EXRs into the canonical render-product files.

    Runs on the worker, inside the AYON publish job that Deadline launches
    after all tile render jobs are done. Resolves oiiotool against the
    worker's local AYON installation (so home-dir / addon-version drift
    between submitter and worker doesn't matter).

    Tile config travels in 'instance.data["publishJobMetadata"]
    ["tile_assembly"]', stashed there by 'submit_publish_job.py' on the
    submitter. Tile EXRs live alongside the canonical file with a
    'TileSuffixPattern % index' suffix inserted before the extension
    (Husk's '--tile-suffix' behaviour).

    Two merge strategies are used depending on the EXR contents:
      - Non-deep tiles use '--add:allsubimages=1'. Husk writes each tile
        with full display window but data window only over the tile rect,
        and pixel data only inside that rect — so summing the four tiles
        is equivalent to compositing, and ':allsubimages=1' keeps every
        AOV subimage (which lack alpha and can't use '--over').
      - Deep tiles use '--deepmerge'. oiiotool's pixel-add ops reject
        deep images outright; '--deepmerge' takes the union of samples
        across the inputs, which is exactly what tiled deep renders need
        (each tile contributes samples only inside its rect).
    """

    label = "Assemble Render Tiles"
    # End of collection: after CollectRenderedFiles (-0.2) populates
    # 'representations', before ValidateExpectedFiles (order 1) checks
    # the canonical files exist on disk.
    order = pyblish.api.CollectorOrder + 0.499
    targets = ["deadline", "farm"]
    families = ["render", "prerender", "renderlayer", "usdrender"]

    def process(self, instance):
        publish_metadata = instance.data.get("publishJobMetadata") or {}
        tile_cfg = publish_metadata.get("tile_assembly")
        if not tile_cfg:
            return

        tiles_x = int(tile_cfg["tilesX"])
        tiles_y = int(tile_cfg["tilesY"])
        suffix_pattern = tile_cfg.get("tileSuffixPattern", "_tile%02d")
        tile_count = tiles_x * tiles_y
        if tile_count < 2:
            return

        oiio_args = get_oiio_tool_args("oiiotool")

        for repre in instance.data.get("representations") or []:
            staging_dir = repre.get("stagingDir")
            if not staging_dir:
                continue
            files = repre.get("files")
            if not files:
                continue
            # 'files' is either a list of basenames (sequences) or a
            # single basename (single-file remainders).
            if isinstance(files, str):
                files = [files]

            # Deep-ness is uniform across all frames of one render
            # product, so probe once per representation off the first
            # frame's first tile.
            first_tile = self._tile_path_for(
                staging_dir, files[0], 0, suffix_pattern
            )
            if not os.path.exists(first_tile):
                raise RuntimeError(
                    "Tile EXR missing for assembly: {}".format(first_tile)
                )
            merge_op = self._choose_merge_op(oiio_args, first_tile)
            self.log.debug(
                "Representation '%s' (%s frames): using %s",
                repre.get("name"), len(files), merge_op,
            )

            for filename in files:
                self._assemble_one(
                    oiio_args,
                    staging_dir,
                    filename,
                    tile_count,
                    suffix_pattern,
                    merge_op,
                )

    def _tile_path_for(self, staging_dir, filename, tile_index, suffix_pattern):
        canonical = os.path.join(staging_dir, filename)
        base, ext = os.path.splitext(canonical)
        return "{}{}{}".format(base, suffix_pattern % tile_index, ext)

    def _choose_merge_op(self, oiio_args, sample_path):
        """Return the oiiotool op string used to combine consecutive tiles.

        Detects deep vs. non-deep via 'oiiotool --info -i:infoformat=xml':
        oiio emits '<deep>1</deep>' in the per-image XML for deep EXRs.
        Plain-text '--info' output doesn't reliably mark deep across
        oiio versions, so we use the XML form (same one AYON's
        'transcoding.py' uses).
        """
        try:
            result = subprocess.run(
                list(oiio_args) + [
                    "--info", "-i:infoformat=xml", sample_path
                ],
                capture_output=True,
                text=True,
                check=True,
            )
        except subprocess.CalledProcessError as exc:
            raise RuntimeError(
                "oiiotool --info failed for '{}': {}".format(
                    sample_path, (exc.stderr or "").strip()
                )
            )
        xml_blob = (result.stdout or "") + (result.stderr or "")
        is_deep = "<deep>1</deep>" in xml_blob
        return "--deepmerge" if is_deep else "--add:allsubimages=1"

    def _assemble_one(
        self,
        oiio_args,
        staging_dir,
        filename,
        tile_count,
        suffix_pattern,
        merge_op,
    ):
        canonical_path = os.path.join(staging_dir, filename)

        cmd = list(oiio_args)
        for i in range(tile_count):
            tile_path = self._tile_path_for(
                staging_dir, filename, i, suffix_pattern
            )
            if not os.path.exists(tile_path):
                raise RuntimeError(
                    "Tile EXR missing for assembly: {}".format(tile_path)
                )
            cmd.append(tile_path)
            if i > 0:
                cmd.append(merge_op)
        cmd.extend(["-o", canonical_path])

        self.log.info(
            "Assembling %s tiles (%s) -> %s",
            tile_count, merge_op, canonical_path,
        )
        try:
            subprocess.run(cmd, check=True)
        except subprocess.CalledProcessError as exc:
            raise RuntimeError(
                "oiiotool tile assembly failed for '{}': exit {}".format(
                    canonical_path, exc.returncode
                )
            )
