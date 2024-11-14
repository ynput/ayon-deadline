import os
import sys
import json
from dataclasses import dataclass, field, asdict
from functools import partial
from typing import Optional, List, Tuple, Any, Dict

import requests

from ayon_core.lib import Logger

# describes list of product typed used for plugin filtering for farm publishing
FARM_FAMILIES = [
    "render", "render.farm", "render.frames_farm",
    "prerender", "prerender.farm", "prerender.frames_farm",
    "renderlayer", "imagesequence", "image",
    "vrayscene", "maxrender",
    "arnold_rop", "mantra_rop",
    "karma_rop", "vray_rop", "redshift_rop",
    "renderFarm", "usdrender", "publish.hou",
    "deadline"
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


def get_deadline_pools(
    webservice_url: str,
    auth: Optional[Tuple[str, str]] = None,
    log: Optional[Logger] = None
) -> List[str]:
    """Get pools from Deadline API.

    Args:
        webservice_url (str): Server url.
        auth (Optional[Tuple[str, str]]): Tuple containing username,
            password
        log (Optional[Logger]): Logger to log errors to, if provided.

    Returns:
        List[str]: Limit Groups.

    Raises:
        RuntimeError: If deadline webservice is unreachable.

    """
    endpoint = "{}/api/pools?NamesOnly=true".format(webservice_url)
    return _get_deadline_info(
        endpoint, auth, log, item_type="pools")


def get_deadline_groups(
    webservice_url: str,
    auth: Optional[Tuple[str, str]] = None,
    log: Optional[Logger] = None
) -> List[str]:
    """Get Groups from Deadline API.

    Args:
        webservice_url (str): Server url.
        auth (Optional[Tuple[str, str]]): Tuple containing username,
            password
        log (Optional[Logger]): Logger to log errors to, if provided.

    Returns:
        List[str]: Limit Groups.

    Raises:
        RuntimeError: If deadline webservice_url is unreachable.

    """
    endpoint = "{}/api/groups".format(webservice_url)
    return _get_deadline_info(
        endpoint, auth, log, item_type="groups")


def get_deadline_limit_groups(
    webservice_url: str,
    auth: Optional[Tuple[str, str]] = None,
    log: Optional[Logger] = None
) -> List[str]:
    """Get Limit Groups from Deadline API.

    Args:
        webservice_url (str): Server url.
        auth (Optional[Tuple[str, str]]): Tuple containing username,
            password
        log (Optional[Logger]): Logger to log errors to, if provided.

    Returns:
        List[str]: Limit Groups.

    Raises:
        RuntimeError: If deadline webservice_url is unreachable.

    """
    endpoint = "{}/api/limitgroups?NamesOnly=true".format(webservice_url)
    return _get_deadline_info(
        endpoint, auth, log, item_type="limitgroups")

def get_deadline_workers(
    webservice_url: str,
    auth: Optional[Tuple[str, str]] = None,
    log: Optional[Logger] = None
) -> List[str]:
    """Get Workers (eg.machine names) from Deadline API.

    Args:
        webservice_url (str): Server url.
        auth (Optional[Tuple[str, str]]): Tuple containing username,
            password
        log (Optional[Logger]): Logger to log errors to, if provided.

    Returns:
        List[str]: Limit Groups.

    Raises:
        RuntimeError: If deadline webservice_url is unreachable.

    """
    endpoint = "{}/api/slaves?NamesOnly=true".format(webservice_url)
    return _get_deadline_info(
        endpoint, auth, log, item_type="workers")


def _get_deadline_info(
    endpoint,
    auth=None,
    log=None,
    item_type=None
):
    from .abstract_submit_deadline import requests_get

    if not log:
        log = Logger.get_logger(__name__)

    try:
        kwargs = {}
        if auth:
            kwargs["auth"] = auth
        response = requests_get(endpoint, **kwargs)
    except requests.exceptions.ConnectionError as exc:
        msg = 'Cannot connect to DL web service {}'.format(endpoint)
        log.error(msg)
        raise(
            DeadlineWebserviceError,
            DeadlineWebserviceError('{} - {}'.format(msg, exc)),
            sys.exc_info()[2]
        )
    if not response.ok:
        log.warning(f"No {item_type} retrieved")
        return []

    return response.json()


class DeadlineWebserviceError(Exception):
    """
    Exception to throw when connection to Deadline server fails.
    """


class DeadlineKeyValueVar(dict):
    """

    Serializes dictionary key values as "{key}={value}" like Deadline uses
    for EnvironmentKeyValue.

    As an example:
        EnvironmentKeyValue0="A_KEY=VALUE_A"
        EnvironmentKeyValue1="OTHER_KEY=VALUE_B"

    The keys are serialized in alphabetical order (sorted).

    Example:
        >>> var = DeadlineKeyValueVar("EnvironmentKeyValue")
        >>> var["my_var"] = "hello"
        >>> var["my_other_var"] = "hello2"
        >>> var.serialize()


    """
    def __init__(self, key):
        super(DeadlineKeyValueVar, self).__init__()
        self.__key = key

    def serialize(self):
        key = self.__key

        # Allow custom location for index in serialized string
        if "{}" not in key:
            key = key + "{}"

        return {
            key.format(index): "{}={}".format(var_key, var_value)
            for index, (var_key, var_value) in enumerate(sorted(self.items()))
        }


class DeadlineIndexedVar(dict):
    """

    Allows to set and query values by integer indices:
        Query: var[1] or var.get(1)
        Set: var[1] = "my_value"
        Append: var += "value"

    Note: Iterating the instance is not guarantueed to be the order of the
          indices. To do so iterate with `sorted()`

    """
    def __init__(self, key):
        super(DeadlineIndexedVar, self).__init__()
        self.__key = key

    def serialize(self):
        key = self.__key

        # Allow custom location for index in serialized string
        if "{}" not in key:
            key = key + "{}"

        return {
            key.format(index): value for index, value in sorted(self.items())
        }

    def next_available_index(self):
        # Add as first unused entry
        i = 0
        while i in self.keys():
            i += 1
        return i

    def update(self, data):
        # Force the integer key check
        for key, value in data.items():
            self.__setitem__(key, value)

    def __iadd__(self, other):
        index = self.next_available_index()
        self[index] = other
        return self

    def __setitem__(self, key, value):
        if not isinstance(key, int):
            raise TypeError("Key must be an integer: {}".format(key))

        if key < 0:
            raise ValueError("Negative index can't be set: {}".format(key))
        dict.__setitem__(self, key, value)


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
    Whitelist: Optional[List[str]] = field(
        default_factory=list)  # Default blank (comma-separated list)
    Blacklist: Optional[List[str]] = field(
        default_factory=list)  # Default blank (comma-separated list)

    # Limits
    LimitGroups: Optional[List[str]] = field(default_factory=list)  # Default: blank

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
    EnvironmentKeyValue: Any = field(
        default_factory=partial(DeadlineKeyValueVar, "EnvironmentKeyValue"))
    IncludeEnvironment: Optional[bool] = field(default=False)  # Default: false
    UseJobEnvironmentOnly: Optional[bool] = field(default=False)  # Default: false
    CustomPluginDirectory: Optional[str] = field(default=None)  # Default blank

    # Job Extra Info
    ExtraInfo: Any = field(
        default_factory=partial(DeadlineIndexedVar, "ExtraInfo"))
    ExtraInfoKeyValue: Any = field(
        default_factory=partial(DeadlineKeyValueVar, "ExtraInfoKeyValue"))

    OverrideTaskExtraInfoNames: Optional[bool] = field(
        default=False)  # Default false

    TaskExtraInfoName: Any = field(
        default_factory=partial(DeadlineIndexedVar, "TaskExtraInfoName"))

    OutputFilename: Any = field(
        default_factory=partial(DeadlineIndexedVar, "OutputFilename"))
    OutputFilenameTile: str = field(
        default_factory=partial(DeadlineIndexedVar, "OutputFilename{}Tile"))
    OutputDirectory: str = field(
        default_factory=partial(DeadlineIndexedVar, "OutputDirectory"))

    AssetDependency: str = field(
        default_factory=partial(DeadlineIndexedVar, "AssetDependency"))

    TileJob: bool = field(default=False)
    TileJobFrame: int = field(default=0)
    TileJobTilesInX: int = field(default=0)
    TileJobTilesInY: int = field(default=0)
    TileJobTileCount: int = field(default=0)

    MaintenanceJob: bool = field(default=False)
    MaintenanceJobStartFrame: int = field(default=0)
    MaintenanceJobEndFrame: int = field(default=0)


@dataclass
class AYONDeadlineJobInfo(DeadlineJobInfo):
    """Contains additional AYON variables from Settings for internal logic."""

    # AYON custom fields used for Settings
    UsePublished: Optional[bool] = field(default=None)
    UseAssetDependencies: Optional[bool] = field(default=None)
    UseWorkfileDependency: Optional[bool] = field(default=None)

    def serialize(self):
        """Return all data serialized as dictionary.

        Returns:
            OrderedDict: all serialized data.

    """
        def filter_data(a, v):
            if isinstance(v, (DeadlineIndexedVar, DeadlineKeyValueVar)):
                return False
            if v is None:
                return False
            return True

        serialized = asdict(self)
        serialized = {
            k: v for k, v in serialized.items()
            if filter_data(k, v)
        }

        # Custom serialize these attributes
        for attribute in [
            self.EnvironmentKeyValue,
            self.ExtraInfo,
            self.ExtraInfoKeyValue,
            self.TaskExtraInfoName,
            self.OutputFilename,
            self.OutputFilenameTile,
            self.OutputDirectory,
            self.AssetDependency
        ]:
            serialized.update(attribute.serialize())

        for attribute_key in [
            "LimitGroups",
            "Whitelist",
            "Blacklist",
        ]:
            serialized[attribute_key] = ",".join(serialized[attribute_key])

        return serialized

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'AYONDeadlineJobInfo':

        implemented_field_values = {
            "ChunkSize": data["chunk_size"],
            "Priority": data["priority"],
            "MachineLimit": data["machine_limit"],
            "ConcurrentTasks": data["concurrent_tasks"],
            "Frames": data.get("frames", ""),
            "Group": cls._sanitize(data["group"]),
            "Pool": cls._sanitize(data["primary_pool"]),
            "SecondaryPool": cls._sanitize(data["secondary_pool"]),

            # fields needed for logic, values unavailable during collection
            "UsePublished": data["use_published"],
            "UseAssetDependencies": data["use_asset_dependencies"],
            "UseWorkfileDependency": data["use_workfile_dependency"]
        }

        return cls(**implemented_field_values)

    def add_render_job_env_var(self):
        """Add required env vars for valid render job submission."""
        for key, value in get_ayon_render_job_envs().items():
            self.EnvironmentKeyValue[key] = value

    def add_instance_job_env_vars(self, instance):
        """Add all job environments as specified on the instance and context

        Any instance `job_env` vars will override the context `job_env` vars.
        """
        for key, value in get_instance_job_envs(instance).items():
            self.EnvironmentKeyValue[key] = value

    def to_json(self) -> str:
        """Serialize the dataclass instance to a JSON string."""
        return json.dumps(asdict(self))

    @staticmethod
    def _sanitize(value) -> str:
        if isinstance(value, str):
            if value == "none":
                return None
            return value
        if isinstance(value, list):
            filtered = []
            for val in value:
                if val and val != "none":
                    filtered.append(val)
            return filtered
