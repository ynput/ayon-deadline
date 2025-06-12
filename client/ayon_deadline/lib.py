from dataclasses import dataclass, field, fields
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
    "remote_publish_on_farm",
    "deadline"
]

# Constant defining where we store job environment variables on instance or
# context data
# DEPRECATED: Use `FARM_JOB_ENV_DATA_KEY` from `ayon_core.pipeline.publish`
#     This variable is NOT USED anywhere in deadline addon.
JOB_ENV_DATA_KEY: str = "farmJobEnv"


@dataclass
class DeadlineConnectionInfo:
    """Connection information for Deadline server."""
    name: str
    url: str
    auth: Tuple[str, str]
    verify: bool


@dataclass
class DeadlineServerInfo:
    pools: List[str]
    limit_groups: List[str]
    groups: List[str]
    machines: List[str]


class DeadlineWebserviceError(Exception):
    """
    Exception to throw when connection to Deadline server fails.
    """


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
    verify: Optional[bool] = None,
    log: Optional[Logger] = None,
) -> List[str]:
    """Get pools from Deadline API.

    Args:
        webservice_url (str): Server url.
        auth (Optional[Tuple[str, str]]): Tuple containing username,
            password
        verify(Optional[bool]): Whether to verify the TLS certificate
            of the Deadline Web Service.
        log (Optional[Logger]): Logger to log errors to, if provided.
    Returns:
        List[str]: Limit Groups.

    Raises:
        RuntimeError: If deadline webservice is unreachable.

    """
    endpoint = f"{webservice_url}/api/pools?NamesOnly=true"
    return _get_deadline_info(endpoint, auth, verify, "pools", log)


def get_deadline_groups(
    webservice_url: str,
    auth: Optional[Tuple[str, str]] = None,
    verify: Optional[bool] = None,
    log: Optional[Logger] = None,
) -> List[str]:
    """Get Groups from Deadline API.

    Args:
        webservice_url (str): Server url.
        auth (Optional[Tuple[str, str]]): Tuple containing username,
            password
        verify(Optional[bool]): Whether to verify the TLS certificate
            of the Deadline Web Service.
        log (Optional[Logger]): Logger to log errors to, if provided.
    Returns:
        List[str]: Limit Groups.

    Raises:
        RuntimeError: If deadline webservice_url is unreachable.

    """
    endpoint = f"{webservice_url}/api/groups"
    return _get_deadline_info(endpoint, auth, verify, "groups", log)


def get_deadline_limit_groups(
    webservice_url: str,
    auth: Optional[Tuple[str, str]] = None,
    verify: Optional[bool] = None,
    log: Optional[Logger] = None,
) -> List[str]:
    """Get Limit Groups from Deadline API.

    Args:
        webservice_url (str): Server url.
        auth (Optional[Tuple[str, str]]): Tuple containing username,
            password
        verify(Optional[bool]): Whether to verify the TLS certificate
            of the Deadline Web Service.
        log (Optional[Logger]): Logger to log errors to, if provided.
    Returns:
        List[str]: Limit Groups.

    Raises:
        RuntimeError: If deadline webservice_url is unreachable.

    """
    endpoint = f"{webservice_url}/api/limitgroups?NamesOnly=true"
    return _get_deadline_info(endpoint, auth, verify, "limitgroups", log)

def get_deadline_workers(
    webservice_url: str,
    auth: Optional[Tuple[str, str]] = None,
    verify: Optional[bool] = None,
    log: Optional[Logger] = None,
) -> List[str]:
    """Get Workers (eg.machine names) from Deadline API.

    Args:
        webservice_url (str): Server url.
        auth (Optional[Tuple[str, str]]): Tuple containing username,
            password
        verify(Optional[bool]): Whether to verify the TLS certificate
            of the Deadline Web Service.
        log (Optional[Logger]): Logger to log errors to, if provided.
    Returns:
        List[str]: Limit Groups.

    Raises:
        RuntimeError: If deadline webservice_url is unreachable.

    """
    endpoint = f"{webservice_url}/api/slaves?NamesOnly=true"
    return _get_deadline_info(endpoint, auth, verify, "workers", log)


def _get_deadline_info(
    endpoint: str,
    auth: Optional[Tuple[str, str]],
    verify: Optional[bool],
    item_type: str,
    log: Optional[Logger],
):
    from .abstract_submit_deadline import requests_get

    if not log:
        log = Logger.get_logger(__name__)

    try:
        kwargs = {}
        if verify is not None:
            kwargs["verify"] = verify
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


# ------------------------------------------------------------
# NOTE It is pipeline related logic from here, probably
#   should be moved to './pipeline' and used from there.
#   - This file is imported in `ayon_deadline/addon.py` which should not
#     have any pipeline logic.
def get_instance_job_envs(instance) -> "dict[str, str]":
    """Add all job environments as specified on the instance and context.

    Any instance `job_env` vars will override the context `job_env` vars.
    """
    # Avoid import from 'ayon_core.pipeline'
    from ayon_core.pipeline.publish import FARM_JOB_ENV_DATA_KEY

    env = {}
    for job_env in [
        instance.context.data.get(FARM_JOB_ENV_DATA_KEY, {}),
        instance.data.get(FARM_JOB_ENV_DATA_KEY, {})
    ]:
        if job_env:
            env.update(job_env)

    # Return the dict sorted just for readability in future logs
    if env:
        env = dict(sorted(env.items()))

    return env


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
    def __init__(self, key: str):
        super().__init__()
        if not key.endswith("{}"):
            key += "{}"
        self._key = key

    def serialize(self):
        # Allow custom location for index in serialized string
        return {
            self._key.format(idx): f"{key}={value}"
            for idx, (key, value) in enumerate(sorted(self.items()))
        }


class DeadlineIndexedVar(dict):
    """

    Allows to set and query values by integer indices:
        Query: var[1] or var.get(1)
        Set: var[1] = "my_value"
        Append: var += "value"

    Note: Iterating the instance is not guaranteed to be the order of the
          indices. To do so iterate with `sorted()`

    """
    def __init__(self, key: str):
        super().__init__()
        if "{}" not in key:
            key += "{}"
        self._key = key

    def serialize(self) -> Dict[str, str]:
        return {
            self._key.format(index): value
            for index, value in sorted(self.items())
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
    # Default: '0'
    Frames: Optional[str] = field(default=None)
    # Default: empty
    Comment: Optional[str] = field(default=None)
    # Default: empty
    Department: Optional[str] = field(default=None)
    # Default: empty
    BatchName: Optional[str] = field(default=None)
    UserName: Optional[str] = field(default=None)
    MachineName: Optional[str] = field(default=None)
    # Default: "none"
    Pool: Optional[str] = field(default=None)
    SecondaryPool: Optional[str] = field(default=None)
    # Default: "none"
    Group: Optional[str] = field(default=None)
    Priority: int = field(default=None)
    ChunkSize: int = field(default=None)
    ConcurrentTasks: int = field(default=None)
    # Default: "true"
    LimitConcurrentTasksToNumberOfCpus: Optional[bool] = field(default=None)
    OnJobComplete: str = field(default=None)
    # Default: false
    SynchronizeAllAuxiliaryFiles: Optional[bool] = field(default=None)
    # Default: false
    ForceReloadPlugin: Optional[bool] = field(default=None)
    # Default: false
    Sequential: Optional[bool] = field(default=None)
    # Default: false
    SuppressEvents: Optional[bool] = field(default=None)
    # Default: false
    Protected: Optional[bool] = field(default=None)
    InitialStatus: "InitialStatus" = field(default="Active")
    NetworkRoot: Optional[str] = field(default=None)

    # Timeouts
    # Default: 0
    MinRenderTimeSeconds: Optional[int] = field(default=None)
    # Default: 0
    MinRenderTimeMinutes: Optional[int] = field(default=None)
    # Default: 0
    TaskTimeoutSeconds: Optional[int] = field(default=None)
    # Default: 0
    TaskTimeoutMinutes: Optional[int] = field(default=None)
    # Default: 0
    StartJobTimeoutSeconds: Optional[int] = field(default=None)
    # Default: 0
    StartJobTimeoutMinutes: Optional[int] = field(default=None)
    # Default: 0
    InitializePluginTimeoutSeconds: Optional[int] = field(default=None)
    # Default: 'Error'
    # Options: 'Error', 'Notify', 'ErrorAndNotify', 'Complete'
    OnTaskTimeout: Optional[str] = field(default=None)
    # Default: false
    EnableTimeoutsForScriptTasks: Optional[bool] = field(default=None)
    # Default: false
    EnableFrameTimeouts: Optional[bool] = field(default=None)
    # Default: false
    EnableAutoTimeout: Optional[bool] = field(default=None)

    # Interruptible
    Interruptible: Optional[bool] = field(default=None)  # Default: false
    InterruptiblePercentage: Optional[int] = field(default=None)
    RemTimeThreshold: Optional[int] = field(default=None)

    # Notifications
    # Default: blank (comma-separated list of users)
    NotificationTargets: Optional[str] = field(default=None)
    # Default: false
    ClearNotificationTargets: Optional[bool] = field(default=None)
    # Default: blank (comma-separated list of email addresses)
    NotificationEmails: Optional[str] = field(default=None)
    OverrideNotificationMethod: Optional[bool] = field(
        default=None)  # Default: false
    # Default: false
    EmailNotification: Optional[bool] = field(default=None)
    # Default: false
    PopupNotification: Optional[bool] = field(default=None)
    # Default: blank
    NotificationNote: Optional[str] = field(default=None)

    # Machine Limit
    # Default: 0
    MachineLimit: Optional[int] = field(default=None)
    # Default -1.0
    MachineLimitProgress: Optional[float] = field(default=None)
    # Default blank (comma-separated list)
    Whitelist: Optional[List[str]] = field(default_factory=list)
    # Default blank (comma-separated list)
    Blacklist: Optional[List[str]] = field(default_factory=list)

    # Limits
    # Default: blank
    LimitGroups: Optional[List[str]] = field(default_factory=list)

    # Dependencies
    # Default: blank
    JobDependencies: List[str] = field(default_factory=list)
    # Default: -1
    JobDependencyPercentage: Optional[int] = field(default=None)
    # Default: false
    IsFrameDependent: Optional[bool] = field(default=None)
    # Default: 0
    FrameDependencyOffsetStart: Optional[int] = field(default=None)
    # Default: 0
    FrameDependencyOffsetEnd: Optional[int] = field(default=None)
    # Default: true
    ResumeOnCompleteDependencies: Optional[bool] = field(default=True)
    # Default: false
    ResumeOnDeletedDependencies: Optional[bool] = field(default=False)
    # Default: false
    ResumeOnFailedDependencies: Optional[bool] = field(default=False)
    # Default: blank (comma-separated list)
    RequiredAssets: Optional[str] = field(default=None)
    # Default: blank (comma-separated list)
    ScriptDependencies: Optional[str] = field(default=None)

    # Failure Detection
    # Default: false
    OverrideJobFailureDetection: Optional[bool] = field(default=False)
    # 0..x
    FailureDetectionJobErrors: Optional[int] = field(default=None)
    # Default: false
    OverrideTaskFailureDetection: Optional[bool] = field(default=False)
    # 0..x
    FailureDetectionTaskErrors: Optional[int] = field(default=None)
    # Default: false
    IgnoreBadJobDetection: Optional[bool] = field(default=False)
    # Default: false
    SendJobErrorWarning: Optional[bool] = field(default=False)

    # Cleanup
    # Default: false
    DeleteOnComplete: Optional[bool] = field(default=False)
    # Default: false
    ArchiveOnComplete: Optional[bool] = field(default=False)
    # Default: false
    OverrideAutoJobCleanup: Optional[bool] = field(default=False)
    OverrideJobCleanup: Optional[bool] = field(default=None)
    # Default: false (not clear)
    JobCleanupDays: Optional[int] = field(default=None)
    OverrideJobCleanupType: Optional[str] = field(default=None)

    # Scheduling
    # Default: 'None'
    # Options: 'None', 'Once', 'Daily', 'Custom'
    ScheduledType: Optional[str] = field(default=None)
    # <dd/MM/yyyy HH:mm>
    ScheduledStartDateTime: Optional[str] = field(default=None)
    # Default: 1
    ScheduledDays: Optional[int] = field(default=1)
    # <dd:hh:mm:ss>
    JobDelay: Optional[str] = field(default=None)
    # <Day of the Week><Start/Stop>Time=<HH:mm:ss>
    Scheduled: Optional[str] = field(default=None)

    # Scripts
    # Default: blank
    PreJobScript: Optional[str] = field(default=None)
    # Default: blank
    PostJobScript: Optional[str] = field(default=None)
    # Default: blank
    PreTaskScript: Optional[str] = field(default=None)
    # Default: blank
    PostTaskScript: Optional[str] = field(default=None)

    # Event Opt-Ins
    # Default blank (comma-separated list)
    EventOptIns: Optional[str] = field(default=None)

    # Environment
    EnvironmentKeyValue: DeadlineKeyValueVar = field(
        default_factory=_partial_key_value("EnvironmentKeyValue"))
    # Default: false
    IncludeEnvironment: Optional[bool] = field(default=False)
    # Default: false
    UseJobEnvironmentOnly: Optional[bool] = field(default=False)
    # Default blank
    CustomPluginDirectory: Optional[str] = field(default=None)

    # Job Extra Info
    ExtraInfo: DeadlineIndexedVar = field(
        default_factory=_partial_indexed("ExtraInfo"))
    ExtraInfoKeyValue: DeadlineKeyValueVar = field(
        default_factory=_partial_key_value("ExtraInfoKeyValue"))

    # Default false
    OverrideTaskExtraInfoNames: Optional[bool] = field(default=False)

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

    def __post_init__(self):
        for attr_name in (
            "JobDependencies",
            "Whitelist",
            "Blacklist",
            "LimitGroups",
        ):
            value = getattr(self, attr_name)
            if value is None:
                continue
            if not isinstance(value, list):
                setattr(self, attr_name, value)

        for attr_name in (
            "ExtraInfo",
            "TaskExtraInfoName",
            "OutputFilename",
            "OutputFilenameTile",
            "OutputDirectory",
            "AssetDependency",
        ):
            value = getattr(self, attr_name)
            if value is None:
                continue
            if not isinstance(value, DeadlineIndexedVar):
                setattr(self, attr_name, value)

        for attr_name in (
            "ExtraInfoKeyValue",
            "EnvironmentKeyValue",
        ):
            value = getattr(self, attr_name)
            if value is None:
                continue
            if not isinstance(value, DeadlineKeyValueVar):
                setattr(self, attr_name, value)

    def __setattr__(self, key, value):
        if value is None:
            super().__setattr__(key, value)
            return

        if key in (
            "JobDependencies",
            "Whitelist",
            "Blacklist",
            "LimitGroups",
        ):
            if isinstance(value, str):
                value = value.split(",")

        elif key in (
            "ExtraInfo",
            "TaskExtraInfoName",
            "OutputFilename",
            "OutputFilenameTile",
            "OutputDirectory",
            "AssetDependency",
        ):
            if not isinstance(value, DeadlineIndexedVar):
                new_value = DeadlineIndexedVar(key)
                new_value.update(value)
                value = new_value

        elif key in (
            "ExtraInfoKeyValue",
            "EnvironmentKeyValue",
        ):
            if not isinstance(value, DeadlineKeyValueVar):
                new_value = DeadlineKeyValueVar(key)
                new_value.update(value)
                value = new_value

        super().__setattr__(key, value)

    def serialize(self):
        """Return all data serialized as dictionary.

        Returns:
            OrderedDict: all serialized data.

        """
        output = {}
        for field_item in fields(self):
            self._fill_serialize_value(
                field_item.name, getattr(self, field_item.name), output
            )
        return output

    def _fill_serialize_value(
        self, key: str, value: Any, output: Dict[str, Any]
    ) -> Any:
        if isinstance(value, (DeadlineIndexedVar, DeadlineKeyValueVar)):
            output.update(value.serialize())
        elif isinstance(value, list):
            output[key] = ",".join(value)
        elif value is not None:
            output[key] = value


@dataclass
class PublishDeadlineJobInfo(DeadlineJobInfo):
    """Contains additional AYON variables from Settings for internal logic."""

    # AYON custom fields used for Settings
    publish_job_state : Optional[str] = field(default=None)
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
            "LimitGroups": cls._sanitize(data["limit_groups"]),
            "Pool": cls._sanitize(data["primary_pool"]),
            "SecondaryPool": cls._sanitize(data["secondary_pool"]),

            # fields needed for logic, values unavailable during collection
            "publish_job_state": data["publish_job_state"],
            "use_published": data["use_published"],
            "use_asset_dependencies": data["use_asset_dependencies"],
            "use_workfile_dependency": data["use_workfile_dependency"],
        })

    def add_render_job_env_var(self):
        """Add required env vars for valid render job submission."""
        self.EnvironmentKeyValue.update(
            JobType.RENDER.get_job_env()
        )

    def add_instance_job_env_vars(self, instance):
        """Add all job environments as specified on the instance and context

        Any instance `job_env` vars will override the context `job_env` vars.
        """
        for key, value in get_instance_job_envs(instance).items():
            self.EnvironmentKeyValue[key] = value

    def _fill_serialize_value(
        self, key: str, value: Any, output: Dict[str, Any]
    ):
        if key not in (
            "publish_job_state",
            "use_published",
            "use_asset_dependencies",
            "use_workfile_dependency",
        ):
            super()._fill_serialize_value(key, value, output)

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
