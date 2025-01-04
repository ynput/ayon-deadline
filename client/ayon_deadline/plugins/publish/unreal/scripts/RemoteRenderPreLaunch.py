import unreal
import RemoteRender

tick_handle = None
executor = None


def initialize_render_job():
    global executor
    executor = RemoteRender.RemoteRenderExecutor()

    mrq_subsystem = unreal.get_editor_subsystem(unreal.MoviePipelineQueueSubsystem)
    mrq_subsystem.render_queue_with_executor_instance(executor)


def wait_for_asset_registry(delta_seconds):
    asset_registry = unreal.AssetRegistryHelpers.get_asset_registry()
    if asset_registry.is_loading_assets():
        unreal.log_warning("Still loading...")
        pass
    else:
        global tick_handle
        unreal.unregister_slate_pre_tick_callback(tick_handle)
        initialize_render_job()


tick_handle = unreal.register_slate_pre_tick_callback(wait_for_asset_registry)
