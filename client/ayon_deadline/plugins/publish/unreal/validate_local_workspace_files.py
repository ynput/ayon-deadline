import pyblish.api
import unreal


class ValidateLocalWorkspaceFiles(pyblish.api.ContextPlugin):
    label = "Validate Local Workspace Files"
    order = pyblish.api.ValidatorOrder
    hosts = ["unreal"]
    families = ["render", "render.farm"]

    def process(self, context):
        pass
