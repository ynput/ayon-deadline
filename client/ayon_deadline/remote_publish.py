import os
from pyblish import util
from ayon_core.lib import Logger


def check_results(context, log):
    error_format = "Failed {{plugin.__name__}}: {{error}}:{{error.traceback}}"
    for result in context.data["results"]:
        if result["success"]:
            continue
        # Error exit as soon as any error occurs.
        error_message = error_format.format(**result)
        log.error(error_message)
        # 'Fatal Error: ' is because of Deadline
        raise RuntimeError("Fatal Error: {{}}".format(error_message))

def remote_publish(log):
    context = util.collect()
    for instance in context:
        if (
            instance.name == os.environ["AYON_INSTANCE_NAME"] and
            instance.data["productType"] == os.environ["AYON_PRODUCT_TYPE"]
        ):
            instance.data["publish"] = True
            instance.data["farm"] = False
        else:
            instance.data["publish"] = False

    check_results(context, log)

    stages = [util.extract, util.integrate]
    for stage in stages:
        stage(context)
        check_results(context, log)


if __name__ == "__main__":
    # Perform remote publish with thorough error checking
    log = Logger.get_logger(__name__)
    remote_publish(log)
