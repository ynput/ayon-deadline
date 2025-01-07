import os
import json
from dataclasses import dataclass, field, asdict
from functools import partial
import typing
from typing import Optional, List, Tuple, Any, Dict, Iterable
from enum import Enum

import requests

from ayon_core.lib import Logger

if typing.TYPE_CHECKING:
    from typing import Union, Self

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


@dataclass
class DeadlineServerInfo:
    pools: List[str]
    limit_groups: List[str]
    groups: List[str]
    machines: List[str]


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


class JobType(str, Enum):
    UNDEFINED = "undefined"
    RENDER = "render"
    PUBLISH = "publish"
    REMOTE = "remote"

    def get_job_env(self) -> Dict[str, str]:
        return {
            "AYON_PUBLISH_JOB": str(int(self == JobType.PUBLISH)),
            "AYON_RENDER_JOB": str(int(self == JobType.RENDER)),
            "AYON_REMOTE_PUBLISH": str(int(self == JobType.REMOTE)),
        }

    @classmethod
    def get(
        cls, value: Any, default: Optional[Any] = None
    ) -> "JobType":
        try:
            return cls(value)
        except ValueError:
            if default is None:
                return cls.UNDEFINED
            return default


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
    endpoint = f"{webservice_url}/api/pools?NamesOnly=true"
    return _get_deadline_info(endpoint, auth, log, "pools")


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
    endpoint = f"{webservice_url}/api/groups"
    return _get_deadline_info(endpoint, auth, log, "groups")


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
    endpoint = f"{webservice_url}/api/limitgroups?NamesOnly=true"
    return _get_deadline_info(endpoint, auth, log, "limitgroups")

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
    endpoint = f"{webservice_url}/api/slaves?NamesOnly=true"
    return _get_deadline_info(endpoint, auth, log, "workers")


def _get_deadline_info(
    endpoint,
    auth,
    log,
    item_type
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
        msg = f"Cannot connect to DL web service {endpoint}"
        log.error(msg)
        raise DeadlineWebserviceError(msg) from exc
    if not response.ok:
        log.warning(f"No {item_type} retrieved")
        return []

    return sorted(response.json(), key=lambda value: (value != "none", value))


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

    def add(self, value: str):
        if value not in self.values():
            self.append(value)

    def append(self, value: str):
        index = self.next_available_index()
        self[index] = value

    def extend(self, values: Iterable[str]):
        for value in values:
            self.append(value)

    def update(self, data: Dict[int, str]):
        # Force the integer key check
        for key, value in data.items():
            self.__setitem__(key, value)

    def __iadd__(self, value: str):
        self.append(value)
        return self

    def __setitem__(self, key, value):
        if not isinstance(key, int):
            raise TypeError(f"Key must be an 'int', got {type(key)} ({key}).")

        if key < 0:
            raise ValueError(f"Negative index can't be set: {key}")
        dict.__setitem__(self, key, value)


def _partial_key_value(key: str):
    return partial(DeadlineKeyValueVar, key)


def _partial_indexed(key: str):
    return partial(DeadlineIndexedVar, key)


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
    UserName: Optional[str] = field(default=None)
    MachineName: Optional[str] = field(default=None)
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
    InitialStatus: "InitialStatus" = field(default="Active")
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
    EnvironmentKeyValue: DeadlineKeyValueVar = field(
        default_factory=_partial_key_value("EnvironmentKeyValue"))
    IncludeEnvironment: Optional[bool] = field(default=False)  # Default: false
    UseJobEnvironmentOnly: Optional[bool] = field(default=False)  # Default: false
    CustomPluginDirectory: Optional[str] = field(default=None)  # Default blank

    # Job Extra Info
    ExtraInfo: DeadlineIndexedVar = field(
        default_factory=_partial_indexed("ExtraInfo"))
    ExtraInfoKeyValue: DeadlineKeyValueVar = field(
        default_factory=_partial_key_value("ExtraInfoKeyValue"))

    OverrideTaskExtraInfoNames: Optional[bool] = field(default=False)  # Default false

    TaskExtraInfoName: DeadlineIndexedVar = field(
        default_factory=_partial_indexed("TaskExtraInfoName"))

    OutputFilename: DeadlineIndexedVar = field(
        default_factory=_partial_indexed("OutputFilename"))
    OutputFilenameTile: DeadlineIndexedVar = field(
        default_factory=_partial_indexed("OutputFilename{}Tile"))
    OutputDirectory: DeadlineIndexedVar = field(
        default_factory=_partial_indexed("OutputDirectory"))

    AssetDependency: DeadlineIndexedVar = field(
        default_factory=_partial_indexed("AssetDependency"))

    TileJob: bool = field(default=False)
    TileJobFrame: int = field(default=0)
    TileJobTilesInX: int = field(default=0)
    TileJobTilesInY: int = field(default=0)
    TileJobTileCount: int = field(default=0)

    MaintenanceJob: bool = field(default=False)
    MaintenanceJobStartFrame: int = field(default=0)
    MaintenanceJobEndFrame: int = field(default=0)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        """Serialize the dataclass instance to a JSON string."""
        return json.dumps(self.to_dict())

    def serialize(self):
        """Return all data serialized as dictionary.

        Returns:
            OrderedDict: all serialized data.

        """
        output = {}
        for key, value in tuple(self.to_dict().items()):
            value = self._serialize_key_value(key, value)
            if value is not None:
                output[key] = value
        return output

    def _serialize_key_value(self, _key: str, value: Any) -> Any:
        if isinstance(value, (DeadlineIndexedVar, DeadlineKeyValueVar)):
            return value.serialize()
        if isinstance(value, list):
            return ",".join(value)
        return value


@dataclass
class PublishDeadlineJobInfo(DeadlineJobInfo):
    """Contains additional AYON variables from Settings for internal logic."""

    # AYON custom fields used for Settings
    use_published: Optional[bool] = field(default=None)
    use_asset_dependencies: Optional[bool] = field(default=None)
    use_workfile_dependency: Optional[bool] = field(default=None)

    @classmethod
    def from_attribute_values(
        cls, data: Dict[str, Any]
    ) -> "Self":
        return cls(**{
            "ChunkSize": data["chunk_size"],
            "Priority": data["priority"],
            "MachineLimit": data["machine_limit"],
            "ConcurrentTasks": data["concurrent_tasks"],
            "Frames": data.get("frames", ""),
            "Group": cls._sanitize(data["group"]),
            "Pool": cls._sanitize(data["primary_pool"]),
            "SecondaryPool": cls._sanitize(data["secondary_pool"]),

            # fields needed for logic, values unavailable during collection
            "use_published": data["use_published"],
            "use_asset_dependencies": data["use_asset_dependencies"],
            "use_workfile_dependency": data["use_workfile_dependency"],
        })

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

    def _serialize_key_value(self, key: str, value: Any):
        if key in (
            "use_published",
            "use_asset_dependencies",
            "use_workfile_dependency",
        ):
            return None
        return super()._serialize_key_value(key, value)

    @staticmethod
    def _sanitize(value) -> "Union[str, List[str], None]":
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
