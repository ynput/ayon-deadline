import pyblish.api
import unreal


class ValidateMRQ(pyblish.api.InstancePlugin):
    label = "Validate Media Render Queue"
    order = pyblish.api.ValidatorOrder
    hosts = ["unreal"]
    families = ["render", "render.farm"]

    def process(self, instance):
        """
        - checks for unsaved assets
        - checks for filled mrq (should assume empty mrq since it will be auto-populated)
        - checks if user has latest p4 changes synced
        """
        self.log.info(f"{instance = }")
        self.curr_mrq = self.get_mrq()
        instance.data["mrq"] = self.curr_mrq

        self.validate_no_dirty_packages()
        self.validate_map()
        self.validate_mrq_has_jobs()

        # TODO: implement p4 checks


    def get_mrq(self):
        mrq_subsystem = unreal.get_editor_subsystem(
            unreal.MoviePipelineQueueSubsystem
        )
        result = mrq_subsystem.get_queue()
        if not result:
            raise Exception("No Media Render Queue found")
        return result


    def validate_no_dirty_packages(self):
        # The user must save their work and check it in so that Deadline can sync it.
        # ? does this check for uncommited files in the default changelist
        dirty_packages = []
        dirty_packages.extend(
            unreal.EditorLoadingAndSavingUtils.get_dirty_content_packages()
        )
        dirty_packages.extend(
            unreal.EditorLoadingAndSavingUtils.get_dirty_map_packages()
        )

        # Sometimes the dialog will return `False` even when there are no packages to save. so we are
        # being explict about the packages we need to save
        if dirty_packages:
            if not unreal.EditorLoadingAndSavingUtils.save_dirty_packages_with_dialog(
                True, True
            ):
                message = (
                    "One or more jobs in the queue have an unsaved map/content. "
                    "\n{packages}\n"
                    "Please save and check-in all work before submission.".format(
                        packages="\n".join(
                            [item.get_name() for item in dirty_packages]
                        )
                    )
                )

                raise Exception(message)

    def validate_map(self):
        is_valid_map = (
            unreal.MoviePipelineEditorLibrary.is_map_valid_for_remote_render(
                self.curr_mrq.get_jobs()
            )
        )
        if not is_valid_map:
            unreal.EditorDialog.show_message(
                "Unsaved Maps",
                "One or more jobs in the queue have an unsaved map as their target map. These unsaved maps cannot be loaded by an external process, and the render has been aborted.",
                unreal.AppMsgType.OK,
            )
            self.on_executor_finished_impl()
            return

    def validate_mrq_has_jobs(self):
        if not len(self.curr_mrq.get_jobs()) > 0:
            raise Exception("Media Render Queue has no jobs to submit")

