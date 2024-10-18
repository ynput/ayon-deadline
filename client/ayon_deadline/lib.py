import os
from dataclasses import dataclass, field
from typing import Optional, Dict, List

# describes list of product typed used for plugin filtering for farm publishing
FARM_FAMILIES = [
    "render", "render.farm", "render.frames_farm",
    "prerender", "prerender.farm", "prerender.frames_farm",
    "renderlayer", "imagesequence", "image",
    "vrayscene", "maxrender",
    "arnold_rop", "mantra_rop",
    "karma_rop", "vray_rop", "redshift_rop",
    "renderFarm", "usdrender", "publish.hou"
]

# Constant defining where we store job environment variables on instance or
# context data
JOB_ENV_DATA_KEY: str = "farmJobEnv"


def get_ayon_render_job_envs() -> "dict[str, str]":
    """Get required env vars for valid render job submission."""
    return {
        "AYON_LOG_NO_COLORS": "1",
        "AYON_RENDER_JOB": "1",
        "AYON_BUNDLE_NAME": os.environ["AYON_BUNDLE_NAME"]
    }


def get_instance_job_envs(instance) -> "dict[str, str]":
    """Add all job environments as specified on the instance and context.

    Any instance `job_env` vars will override the context `job_env` vars.
    """
    env = {}
    for job_env in [
        instance.context.data.get(JOB_ENV_DATA_KEY, {}),
        instance.data.get(JOB_ENV_DATA_KEY, {})
    ]:
        if job_env:
            env.update(job_env)

    # Return the dict sorted just for readability in future logs
    if env:
        env = dict(sorted(env.items()))

    return env


@dataclass
class DeadlineJobInfo:
    """Mapping of all Deadline JobInfo attributes.

    This contains all JobInfo attributes plus their default values.
    Those attributes set to `None` shouldn't be posted to Deadline as
    the only required one is `Plugin`.
    """

    # Required
    Plugin: str = field(default="Untitled")

    # General
    Name: str = field(default="Untitled")
    Frames: Optional[int] = field(default=None)  # default: 0
    Comment: Optional[str] = field(default=None)  # default: empty
    Department: Optional[str] = field(default=None)  # default: empty
    BatchName: Optional[str] = field(default=None)  # default: empty
    UserName: str = field(default=None)
    MachineName: str = field(default=None)
    Pool: Optional[str] = field(default=None)  # default: "none"
    SecondaryPool: Optional[str] = field(default=None)
    Group: Optional[str] = field(default=None)  # default: "none"
    Priority: int = field(default=None)
    ChunkSize: int = field(default=None)
    ConcurrentTasks: int = field(default=None)
    LimitConcurrentTasksToNumberOfCpus: Optional[bool] = field(
        default=None)  # default: "true"
    OnJobComplete: str = field(default=None)
    SynchronizeAllAuxiliaryFiles: Optional[bool] = field(
        default=None)  # default: false
    ForceReloadPlugin: Optional[bool] = field(default=None)  # default: false
    Sequential: Optional[bool] = field(default=None)  # default: false
    SuppressEvents: Optional[bool] = field(default=None)  # default: false
    Protected: Optional[bool] = field(default=None)  # default: false
    InitialStatus: str = field(default="Active")
    NetworkRoot: Optional[str] = field(default=None)

    # Timeouts
    MinRenderTimeSeconds: Optional[int] = field(default=None)  # Default: 0
    MinRenderTimeMinutes: Optional[int] = field(default=None)  # Default: 0
    TaskTimeoutSeconds: Optional[int] = field(default=None)  # Default: 0
    TaskTimeoutMinutes: Optional[int] = field(default=None)  # Default: 0
    StartJobTimeoutSeconds: Optional[int] = field(default=None)  # Default: 0
    StartJobTimeoutMinutes: Optional[int] = field(default=None)  # Default: 0
    InitializePluginTimeoutSeconds: Optional[int] = field(
        default=None)  # Default: 0
    OnTaskTimeout: Optional[str] = field(default=None)  # Default: Error
    EnableTimeoutsForScriptTasks: Optional[bool] = field(
        default=None)  # Default: false
    EnableFrameTimeouts: Optional[bool] = field(default=None)  # Default: false
    EnableAutoTimeout: Optional[bool] = field(default=None)  # Default: false

    # Interruptible
    Interruptible: Optional[bool] = field(default=None)  # Default: false
    InterruptiblePercentage: Optional[int] = field(default=None)
    RemTimeThreshold: Optional[int] = field(default=None)

    # Notifications
    NotificationTargets: Optional[str] = field(
        default=None)  # Default: blank (comma-separated list of users)
    ClearNotificationTargets: Optional[bool] = field(
        default=None)  # Default: false
    NotificationEmails: Optional[str] = field(
        default=None)  # Default: blank (comma-separated list of email addresses)
    OverrideNotificationMethod: Optional[bool] = field(
        default=None)  # Default: false
    EmailNotification: Optional[bool] = field(default=None)  # Default: false
    PopupNotification: Optional[bool] = field(default=None)  # Default: false
    NotificationNote: Optional[str] = field(default=None)  # Default: blank

    # Machine Limit
    MachineLimit: Optional[int] = field(default=None)  # Default: 0
    MachineLimitProgress: Optional[float] = field(default=None)  # Default -1.0
    Whitelist: Optional[str] = field(
        default=None)  # Default blank (comma-separated list)
    Blacklist: Optional[str] = field(
        default=None)  # Default blank (comma-separated list)

    # Limits
    LimitGroups: Optional[str] = field(default=None)  # Default: blank

    # Dependencies
    JobDependencies: Optional[str] = field(default=None)  # Default: blank
    JobDependencyPercentage: Optional[int] = field(default=None)  # Default: -1
    IsFrameDependent: Optional[bool] = field(default=None)  # Default: false
    FrameDependencyOffsetStart: Optional[int] = field(default=None)  # Default: 0
    FrameDependencyOffsetEnd: Optional[int] = field(default=None)  # Default: 0
    ResumeOnCompleteDependencies: Optional[bool] = field(
        default=True)  # Default: true
    ResumeOnDeletedDependencies: Optional[bool] = field(
        default=False)  # Default: false
    ResumeOnFailedDependencies: Optional[bool] = field(
        default=False)  # Default: false
    RequiredAssets: Optional[str] = field(
        default=None)  # Default: blank (comma-separated list)
    ScriptDependencies: Optional[str] = field(
        default=None)  # Default: blank (comma-separated list)

    # Failure Detection
    OverrideJobFailureDetection: Optional[bool] = field(
        default=False)  # Default: false
    FailureDetectionJobErrors: Optional[int] = field(default=None)  # 0..x
    OverrideTaskFailureDetection: Optional[bool] = field(
        default=False)  # Default: false
    FailureDetectionTaskErrors: Optional[int] = field(default=None)  # 0..x
    IgnoreBadJobDetection: Optional[bool] = field(
        default=False)  # Default: false
    SendJobErrorWarning: Optional[bool] = field(
        default=False)  # Default: false

    # Cleanup
    DeleteOnComplete: Optional[bool] = field(default=False)  # Default: false
    ArchiveOnComplete: Optional[bool] = field(default=False)  # Default: false
    OverrideAutoJobCleanup: Optional[bool] = field(
        default=False)  # Default: false
    OverrideJobCleanup: Optional[bool] = field(default=None)
    JobCleanupDays: Optional[int] = field(
        default=None)  # Default: false (not clear)
    OverrideJobCleanupType: Optional[str] = field(default=None)

    # Scheduling
    ScheduledType: Optional[str] = field(
        default=None)  # Default: None (<None/Once/Daily/Custom>)
    ScheduledStartDateTime: Optional[str] = field(
        default=None)  # <dd/MM/yyyy HH:mm>
    ScheduledDays: Optional[int] = field(default=1)  # Default: 1
    JobDelay: Optional[str] = field(default=None)  # <dd:hh:mm:ss>
    Scheduled: Optional[str] = field(
        default=None)  # <Day of the Week><Start/Stop>Time=<HH:mm:ss>

    # Scripts
    PreJobScript: Optional[str] = field(default=None)  # Default: blank
    PostJobScript: Optional[str] = field(default=None)  # Default: blank
    PreTaskScript: Optional[str] = field(default=None)  # Default: blank
    PostTaskScript: Optional[str] = field(default=None)  # Default: blank

    # Event Opt-Ins
    EventOptIns: Optional[str] = field(
        default=None)  # Default blank (comma-separated list)

    # Environment
    EnvironmentKeyValue: str = field(default_factory=lambda: "EnvironmentKeyValue")
    IncludeEnvironment: Optional[bool] = field(default=False)  # Default: false
    UseJobEnvironmentOnly: Optional[bool] = field(
        default=False)  # Default: false
    CustomPluginDirectory: Optional[str] = field(default=None)  # Default blank

    # Job Extra Info
    ExtraInfoKeyValue: str = field(default_factory=lambda: "ExtraInfoKeyValue")

    OverrideTaskExtraInfoNames: Optional[bool] = field(
        default=False)  # Default false

    TaskExtraInfoName: str = field(default_factory=lambda: "TaskExtraInfoName")

    OutputFilename: str = field(default_factory=lambda: "OutputFilename")
    OutputFilenameTile: str = field(default_factory=lambda: "OutputFilename{}Tile")
    OutputDirectory: str = field(default_factory=lambda: "OutputDirectory")

    AssetDependency: str = field(default_factory=lambda: "AssetDependency")

    TileJob: bool = field(default=False)
    TileJobFrame: int = field(default=0)
    TileJobTilesInX: int = field(default=0)
    TileJobTilesInY: int = field(default=0)
    TileJobTileCount: int = field(default=0)

    MaintenanceJob: bool = field(default=False)
    MaintenanceJobStartFrame: int = field(default=0)
    MaintenanceJobEndFrame: int = field(default=0)


    @classmethod
    def from_dict(cls, data: Dict) -> 'JobInfo':

        def capitalize(key):
            words = key.split("_")
            return "".join(word.capitalize() for word in words)

        # Filter the dictionary to only include keys that are fields in the dataclass
        capitalized = {capitalize(k): v for k, v in data.items()}
        filtered_data = {k: v for k, v
                         in capitalized.items()
                         if k in cls.__annotations__}
        return cls(**filtered_data)



arr = {"priority": 40}
job = DeadlineJobInfo.from_dict(arr)
print(job.Priority)