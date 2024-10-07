import pyblish.api

try:
    from ayon_usd import get_usd_pinning_envs
    HAS_AYON_USD = True
except ImportError:
    HAS_AYON_USD = False

    # usd is not enabled or available, so we just mock the function
    def get_usd_pinning_envs(instance):
        return {}


class CollectUSDPinningEnvVars(pyblish.api.InstancePlugin):
    # TODO: This plug-in should actually make its way to ayon-usd addon
    order = pyblish.api.CollectorOrder + 0.250
    label = "Collect USD Pinning Env vars (Deadline Job)"

    enabled = HAS_AYON_USD
    families = [
        # Maya
        "renderlayer",
        # Houdini
        "publish.hou",  # cache submissions
        "redshift_rop",
        "arnold_rop",
        "mantra_rop",
        "karma_rop",
        "vray_rop"
    ]
    targets = ["local"]

    # USD Pinning file generation only supported for Maya and Houdini currently
    hosts = ["maya",
             "houdini"]

    def process(self, instance):
        if not instance.data.get("farm"):
            self.log.debug("Should not be processed on farm, skipping.")
            return

        job_env = instance.data.setdefault("job_env", {})
        job_env.update(get_usd_pinning_envs(instance))
