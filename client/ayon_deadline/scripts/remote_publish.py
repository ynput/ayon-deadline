import os
import pyblish.api
import pyblish.util

from ayon_core.lib import Logger
from ayon_core.pipeline.create import CreateContext
from ayon_core.pipeline import registered_host


def remote_publish(log):
    error_format = "Failed {{plugin.__name__}}: {{error}}:{{error.traceback}}"
    host = registered_host()
    create_context = CreateContext(host)
    pyblish_context = pyblish.api.Context()
    solo_instance_ids: set[str] = set(
        os.environ.get("INSTANCE_IDS", "").split(";")
    )
    for instance in create_context.instances:
        active: bool = instance.id in solo_instance_ids
        log.info(f"Setting active state {active} for instance: {instance}")
        instance["active"] = active

        pyblish_context.data["create_context"] = create_context

    for result in pyblish.util.publish_iter(
        context=pyblish_context,
        plugins=create_context.publish_plugins

    ):
        if result["error"]:
            error_message = error_format.format(**result)
            log.error(error_message)
            raise RuntimeError("Fatal Error : {}".format(error_message))


if __name__ == "__main__":
    # Perform remote publish with thorough error checking
    log = Logger.get_logger(__name__)
    remote_publish(log)
