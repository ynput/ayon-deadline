import pyblish.api

from ayon_core.pipeline import (
    PublishXmlValidationError,
    OptionalPyblishPluginMixin
)
from ayon_deadline.lib import FARM_FAMILIES, get_deadline_pools


class ValidateDeadlinePools(OptionalPyblishPluginMixin,
                            pyblish.api.InstancePlugin):
    """Validate primaryPool and secondaryPool on instance.

    Values are on instance based on value insertion when Creating instance or
    by Settings in CollectDeadlinePools.
    """

    label = "Validate Deadline Pools"
    order = pyblish.api.ValidatorOrder
    families = FARM_FAMILIES
    optional = True
    targets = ["local"]

    # cache
    pools_by_url = {}

    def process(self, instance):
        if not self.is_active(instance.data):
            return

        if not instance.data.get("farm"):
            self.log.debug("Skipping local instance.")
            return

        deadline_url = instance.data["deadline"]["url"]
        addons_manager = instance.context.data["ayonAddonsManager"]
        deadline_addon = addons_manager["deadline"]
        pools = self.get_pools(
            deadline_addon,
            deadline_url,
            instance.data["deadline"].get("auth"),
            instance.data["deadline"]["verify"]
        )

        invalid_pools = {}
        job_info = instance.data["deadline"]["job_info"]
        primary_pool = job_info.Pool
        if primary_pool and primary_pool not in pools:
            invalid_pools["primary"] = primary_pool

        secondary_pool = job_info.SecondaryPool
        if secondary_pool and secondary_pool not in pools:
            invalid_pools["secondary"] = secondary_pool

        if invalid_pools:
            message = "\n".join(
                "{} pool '{}' not available on Deadline".format(key.title(),
                                                                pool)
                for key, pool in invalid_pools.items()
            )
            raise PublishXmlValidationError(
                plugin=self,
                message=message,
                formatting_data={"pools_str": ", ".join(pools)}
            )

    def get_pools(self, deadline_addon, deadline_url, auth, verify):
        if deadline_url not in self.pools_by_url:
            self.log.debug(
                "Querying available pools for Deadline url: {}".format(
                    deadline_url)
            )
            pools = get_deadline_pools(
                deadline_url, auth=auth, log=self.log, verify=verify
            )
            # some DL return "none" as a pool name
            if "none" not in pools:
                pools.append("none")
            self.log.info("Available pools: {}".format(pools))
            self.pools_by_url[deadline_url] = pools

        return self.pools_by_url[deadline_url]
