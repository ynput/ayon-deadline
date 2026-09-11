# /usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import tempfile
from datetime import datetime
import subprocess
import json
import platform
import uuid
import re
from time import sleep, time
import getpass
from hashlib import sha256

from Deadline.Scripting import (
    RepositoryUtils,
    FileUtils,
    DirectoryUtils,
)


__version__ = "1.2.6"
VERSION_REGEX = re.compile(
    r"(?P<major>0|[1-9]\d*)"
    r"\.(?P<minor>0|[1-9]\d*)"
    r"\.(?P<patch>0|[1-9]\d*)"
    r"(?:-(?P<prerelease>[a-zA-Z\d\-.]*))?"
    r"(?:\+(?P<buildmetadata>[a-zA-Z\d\-.]*))?"
)
# Seconds a worker waits for another worker to extract the environment cache
# before it stops waiting and extracts the environment just for itself.
EXTRACT_ENVIRONMENT_TIMEOUT = 10
# Seconds between checks while waiting for another worker.
ENV_CACHE_POLL_INTERVAL = 0.5
# Age at which an extraction lock file is considered left behind by a worker
# that will never come back, e.g. because its machine lost power.
ENV_CACHE_LOCK_STALE_TIMEOUT = 300
# Results of '_acquire_env_cache_lock'.
_LOCK_ACQUIRED = "acquired"
_LOCK_CACHE_READY = "cache_ready"
_LOCK_UNAVAILABLE = "unavailable"


class OpenPypeVersion:
    """Fake semver version class for OpenPype version purposes.

    The version
    """
    def __init__(self, major, minor, patch, prerelease, origin=None):
        self.major = major
        self.minor = minor
        self.patch = patch
        self.prerelease = prerelease

        is_valid = True
        if major is None or minor is None or patch is None:
            is_valid = False
        self.is_valid = is_valid

        if origin is None:
            base = "{}.{}.{}".format(str(major), str(minor), str(patch))
            if not prerelease:
                origin = base
            else:
                origin = "{}-{}".format(base, str(prerelease))

        self.origin = origin

    @classmethod
    def from_string(cls, version):
        """Create an object of version from string.

        Args:
            version (str): Version as a string.

        Returns:
            Union[OpenPypeVersion, None]: Version object if input is nonempty
                string otherwise None.
        """

        if not version:
            return None
        valid_parts = VERSION_REGEX.findall(version)
        if len(valid_parts) != 1:
            # Return invalid version with filled 'origin' attribute
            return cls(None, None, None, None, origin=str(version))

        # Unpack found version
        major, minor, patch, pre, post = valid_parts[0]
        prerelease = pre
        # Post release is not important anymore and should be considered as
        #   part of prerelease
        # - comparison is implemented to find suitable build and builds should
        #       never contain prerelease part so "not proper" parsing is
        #       acceptable for this use case.
        if post:
            prerelease = "{}+{}".format(pre, post)

        return cls(
            int(major), int(minor), int(patch), prerelease, origin=version
        )

    def has_compatible_release(self, other):
        """Version has compatible release as other version.

        Both major and minor versions must be exactly the same. In that case
        a build can be considered as release compatible with any version.

        Args:
            other (OpenPypeVersion): Other version.

        Returns:
            bool: Version is release compatible with other version.
        """

        if self.is_valid and other.is_valid:
            return self.major == other.major and self.minor == other.minor
        return False

    def __bool__(self):
        return self.is_valid

    def __repr__(self):
        return "<{} {}>".format(self.__class__.__name__, self.origin)

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return self.origin == other
        return self.origin == other.origin

    def __lt__(self, other):
        if not isinstance(other, self.__class__):
            return None

        if not self.is_valid:
            return True

        if not other.is_valid:
            return False

        if self.origin == other.origin:
            return None

        same_major = self.major == other.major
        if not same_major:
            return self.major < other.major

        same_minor = self.minor == other.minor
        if not same_minor:
            return self.minor < other.minor

        same_patch = self.patch == other.patch
        if not same_patch:
            return self.patch < other.patch

        if not self.prerelease:
            return False

        if not other.prerelease:
            return True

        pres = [self.prerelease, other.prerelease]
        pres.sort()
        return pres[0] == self.prerelease


def get_openpype_version_from_path(path, build=True):
    """Get OpenPype version from provided path.
         path (str): Path to scan.
         build (bool, optional): Get only builds, not sources

    Returns:
        Union[OpenPypeVersion, None]: version of OpenPype if found.
    """

    # fix path for application bundle on macos
    if platform.system().lower() == "darwin":
        path = os.path.join(path, "MacOS")

    version_file = os.path.join(path, "openpype", "version.py")
    if not os.path.isfile(version_file):
        return None

    # skip if the version is not build
    exe = os.path.join(path, "openpype_console.exe")
    if platform.system().lower() in ["linux", "darwin"]:
        exe = os.path.join(path, "openpype_console")

    # if only builds are requested
    if build and not os.path.isfile(exe):  # noqa: E501
        print("   ! path is not a build: {}".format(path))
        return None

    version = {}
    with open(version_file, "r") as vf:
        exec(vf.read(), version)

    version_str = version.get("__version__")
    if version_str:
        return OpenPypeVersion.from_string(version_str)
    return None


def get_openpype_executable():
    """Return OpenPype Executable from Event Plug-in Settings"""
    config = RepositoryUtils.GetPluginConfig("OpenPype")
    exe_list = config.GetConfigEntryWithDefault("OpenPypeExecutable", "")
    dir_list = config.GetConfigEntryWithDefault(
        "OpenPypeInstallationDirs", "")

    # clean '\ ' for MacOS pasting
    if platform.system().lower() == "darwin":
        exe_list = exe_list.replace("\\ ", " ")
        dir_list = dir_list.replace("\\ ", " ")
    return exe_list, dir_list


def get_openpype_versions(dir_list):
    print(">>> Getting OpenPype executable ...")
    openpype_versions = []

    # special case of multiple install dirs
    for dir_list in dir_list.split(","):
        install_dir = DirectoryUtils.SearchDirectoryList(dir_list)
        if install_dir:
            print("--- Looking for OpenPype at: {}".format(install_dir))
            sub_dirs = [
                f.path for f in os.scandir(install_dir)
                if f.is_dir()
            ]
            for subdir in sub_dirs:
                version = get_openpype_version_from_path(subdir)
                if not version:
                    continue
                print("  - found: {} - {}".format(version, subdir))
                openpype_versions.append((version, subdir))
    return openpype_versions


def get_requested_openpype_executable(
    exe, dir_list, requested_version
):
    requested_version_obj = OpenPypeVersion.from_string(requested_version)
    if not requested_version_obj:
        print((
            ">>> Requested version '{}' does not match version regex '{}'"
        ).format(requested_version, VERSION_REGEX))
        return None

    print((
        ">>> Scanning for compatible requested version {}"
    ).format(requested_version))
    openpype_versions = get_openpype_versions(dir_list)
    if not openpype_versions:
        return None

    # if looking for requested compatible version,
    # add the implicitly specified to the list too.
    if exe:
        exe_dir = os.path.dirname(exe)
        print("Looking for OpenPype at: {}".format(exe_dir))
        version = get_openpype_version_from_path(exe_dir)
        if version:
            print("  - found: {} - {}".format(version, exe_dir))
            openpype_versions.append((version, exe_dir))

    matching_item = None
    compatible_versions = []
    for version_item in openpype_versions:
        version, version_dir = version_item
        if requested_version_obj.has_compatible_release(version):
            compatible_versions.append(version_item)
            if version == requested_version_obj:
                # Store version item if version match exactly
                # - break if is found matching version
                matching_item = version_item
                break

    if not compatible_versions:
        return None

    compatible_versions.sort(key=lambda item: item[0])
    if matching_item:
        version, version_dir = matching_item
        print((
            "*** Found exact match build version {} in {}"
        ).format(version_dir, version))

    else:
        version, version_dir = compatible_versions[-1]

        print((
            "*** Latest compatible version found is {} in {}"
        ).format(version_dir, version))

    # create list of executables for different platform and let
    # Deadline decide.
    exe_list = [
        os.path.join(version_dir, "openpype_console.exe"),
        os.path.join(version_dir, "openpype_console"),
        os.path.join(version_dir, "MacOS", "openpype_console")
    ]
    return FileUtils.SearchFileList(";".join(exe_list))


def inject_openpype_environment(deadlinePlugin):
    """ Pull env vars from OpenPype and push them to rendering process.

        Used for correct paths, configuration from OpenPype etc.
    """
    job = deadlinePlugin.GetJob()

    print(">>> Injecting OpenPype environments ...")
    try:
        exe_list, dir_list = get_openpype_executable()
        exe = FileUtils.SearchFileList(exe_list)

        requested_version = job.GetJobEnvironmentKeyValue("OPENPYPE_VERSION")
        if requested_version:
            exe = get_requested_openpype_executable(
                exe, dir_list, requested_version
            )
            if exe is None:
                raise RuntimeError((
                    "Cannot find compatible version available for version {}"
                    " requested by the job. Please add it through plugin"
                    " configuration in Deadline or install it to configured"
                    " directory."
                ).format(requested_version))

        if not exe:
            raise RuntimeError((
                "OpenPype executable was not found in the semicolon "
                "separated list \"{}\"."
                "The path to the render executable can be configured"
                " from the Plugin Configuration in the Deadline Monitor."
            ).format(";".join(exe_list)))

        print("--- OpenPype executable: {}".format(exe))

        # tempfile.TemporaryFile cannot be used because of locking
        temp_file_name = "{}_{}.json".format(
            datetime.utcnow().strftime("%Y%m%d%H%M%S%f"),
            str(uuid.uuid1())
        )
        export_path = os.path.join(tempfile.gettempdir(), temp_file_name)
        print(">>> Temporary path: {}".format(export_path))

        args = [
            "--headless",
            "extractenvironments",
            export_path
        ]

        add_kwargs = {
            "project": job.GetJobEnvironmentKeyValue("AVALON_PROJECT"),
            "asset": job.GetJobEnvironmentKeyValue("AVALON_ASSET"),
            "task": job.GetJobEnvironmentKeyValue("AVALON_TASK"),
            "app": job.GetJobEnvironmentKeyValue("AVALON_APP_NAME"),
            "envgroup": "farm"
        }

        # use legacy IS_TEST env var to mark automatic tests for OP
        if job.GetJobEnvironmentKeyValue("IS_TEST"):
            args.append("--automatic-tests")

        if all(add_kwargs.values()):
            for key, value in add_kwargs.items():
                args.extend(["--{}".format(key), value])
        else:
            raise RuntimeError((
                "Missing required env vars: AVALON_PROJECT, AVALON_ASSET,"
                " AVALON_TASK, AVALON_APP_NAME"
            ))

        openpype_mongo = job.GetJobEnvironmentKeyValue("OPENPYPE_MONGO")
        if openpype_mongo:
            # inject env var for OP extractenvironments
            # SetEnvironmentVariable is important, not SetProcessEnv...
            deadlinePlugin.SetEnvironmentVariable("OPENPYPE_MONGO",
                                                  openpype_mongo)

        if not os.environ.get("OPENPYPE_MONGO"):
            print(">>> Missing OPENPYPE_MONGO env var, process won't work")

        os.environ["AVALON_TIMEOUT"] = "5000"

        args_str = subprocess.list2cmdline(args)
        print(">>> Executing: {} {}".format(exe, args_str))
        process_exitcode = deadlinePlugin.RunProcess(
            exe, args_str, os.path.dirname(exe), -1
        )

        if process_exitcode != 0:
            raise RuntimeError(
                "Failed to run OpenPype process to extract environments."
            )

        print(">>> Loading file ...")
        with open(export_path) as fp:
            contents = json.load(fp)

        for key, value in sorted(contents.items()):
            deadlinePlugin.SetProcessEnvironmentVariable(key, value)

        if "PATH" in contents:
            # Set os.environ[PATH] so studio settings' path entries
            # can be used to define search path for executables.
            print(f">>> Setting 'PATH' Environment to: {contents['PATH']}")
            os.environ["PATH"] = contents["PATH"]

        script_url = job.GetJobPluginInfoKeyValue("ScriptFilename")
        if script_url:
            script_url = script_url.format(**contents).replace("\\", "/")
            print(">>> Setting script path {}".format(script_url))
            job.SetJobPluginInfoKeyValue("ScriptFilename", script_url)

        print(">>> Removing temporary file")
        os.remove(export_path)

        print(">> Injection end.")
    except Exception as e:
        if hasattr(e, "output"):
            print(">>> Exception {}".format(e.output))
        import traceback
        print(traceback.format_exc())
        print("!!! Injection failed.")
        raise


def inject_ayon_environment(deadlinePlugin):
    """ Pull env vars from AYON and push them to rendering process.

        Used for correct paths, configuration from AYON etc.
    """
    job = deadlinePlugin.GetJob()

    print(">>> Injecting AYON environments ...")
    try:
        exe_list = get_ayon_executable()
        exe = FileUtils.SearchFileList(exe_list)

        if not exe:
            raise RuntimeError((
               "AYON executable was not found in the semicolon "
               "separated list \"{}\"."
               "The path to the render executable can be configured"
               " from the Plugin Configuration in the Deadline Monitor."
            ).format(exe_list))

        print("--- AYON executable: {}".format(exe))

        ayon_bundle_name = job.GetJobEnvironmentKeyValue("AYON_BUNDLE_NAME")
        ayon_studio_bundle_name = job.GetJobEnvironmentKeyValue(
            "AYON_STUDIO_BUNDLE_NAME"
        )
        if not ayon_studio_bundle_name:
            ayon_studio_bundle_name = ayon_bundle_name

        if not ayon_studio_bundle_name:
            raise RuntimeError(
                "Missing env var in job properties AYON_STUDIO_BUNDLE_NAME"
            )

        ayon_server_url, ayon_api_key = handle_credentials(job)

        site_id = os.environ.get("AYON_SITE_ID")
        shared_env_group = None
        if site_id:
            hash_base = f"{site_id}|{getpass.getuser()}"
            hash_sha256 = sha256(hash_base.encode())
            shared_env_group = hash_sha256.hexdigest()[-10:]

        def extract_to(target_path):
            """Run the extraction process writing into 'target_path'."""
            _extract_environments(
                ayon_server_url,
                ayon_api_key,
                ayon_studio_bundle_name,
                ayon_bundle_name,
                deadlinePlugin,
                exe,
                target_path,
                job
            )

        contents = None
        # drive caching of environment variables with env var
        # it is recommended to use same value AYON_SITE_ID for 'same'
        # render nodes (eg. same OS etc.)
        if shared_env_group:
            print(">>> Caching of environment file will be used.")
            export_path = _get_env_cache_path(job, shared_env_group)
            if export_path:
                contents = _get_shared_environments(
                    export_path,
                    _get_env_cache_timeout(job),
                    extract_to
                )

        if contents is None:
            # Either caching is disabled or the shared cache could not be
            # used, e.g. because another worker is still busy with it. That
            # must never fail the job, so extract into a file private to
            # this worker instead.
            contents = _extract_private_environments(extract_to)

        for key, value in sorted(contents.items()):
            deadlinePlugin.SetProcessEnvironmentVariable(key, value)

        if "PATH" in contents:
            # Set os.environ[PATH] so studio settings' path entries
            # can be used to define search path for executables.
            print(f">>> Setting 'PATH' Environment to: {contents['PATH']}")
            os.environ["PATH"] = contents["PATH"]

        script_url = job.GetJobPluginInfoKeyValue("ScriptFilename")
        if script_url:
            script_url = script_url.format(**contents).replace("\\", "/")
            print(">>> Setting script path {}".format(script_url))
            job.SetJobPluginInfoKeyValue("ScriptFilename", script_url)

        print(">> Injection end.")
    except Exception as e:
        if hasattr(e, "output"):
            print(">>> Exception {}".format(e.output))
        import traceback
        print(traceback.format_exc())
        print("!!! Injection failed.")
        raise


def _get_env_cache_timeout(job):
    """How long a worker waits for the environment extraction of another one.

    Returns:
        int: Wait time in seconds, overridable per job with the
            'AYON_EXTRACT_ENVIRONMENT_TIMEOUT' environment variable.
    """
    value = job.GetJobEnvironmentKeyValue("AYON_EXTRACT_ENVIRONMENT_TIMEOUT")
    try:
        timeout = int(value)
    except (TypeError, ValueError):
        timeout = EXTRACT_ENVIRONMENT_TIMEOUT
    return max(timeout, 0)


def _get_env_cache_path(job, shared_env_group):
    """Path of the environment cache file shared by the workers of a job.

    Args:
        job: Deadline job.
        shared_env_group (str): Hash of the site id and user, so that only
            workers with a matching environment share a cache file.

    Returns:
        Optional[str]: Path of the shared JSON cache file. 'None' when its
            location can't be determined or created, in which case the caller
            falls back to a worker private environment file.
    """
    try:
        export_dir_url = os.path.join(_get_output_dir(job), ".ayon_env_cache")
        # 'exist_ok' keeps this safe when many workers start at once
        os.makedirs(export_dir_url, exist_ok=True)
    except (OSError, RuntimeError) as exc:
        print(f">>> Unable to prepare environment cache folder: {exc}")
        return None

    return os.path.join(
        export_dir_url, f"env_{job.JobId}_{shared_env_group}.json"
    )


def _get_shared_environments(export_path, timeout, extract_to):
    """Get environments through the cache file shared by the job's workers.

    Only a single worker runs the expensive extraction process, all others
    wait for its result. Problems with the shared cache itself - a lock that
    can't be taken, a worker that takes too long, an unreadable file - return
    'None' instead of raising, so the caller can fall back to a private
    extraction. A failure of the extraction process itself is raised.

    Args:
        export_path (str): Path to the shared cache file.
        timeout (int): Seconds to wait for another worker before giving up.
        extract_to (Callable[[str], None]): Runs the extraction process into
            the path it receives.

    Returns:
        Optional[dict[str, str]]: Extracted environment variables.
    """
    status, token = _acquire_env_cache_lock(export_path, timeout)
    if status == _LOCK_ACQUIRED:
        try:
            # Re-check: another worker may have completed the extraction
            # between our last check and acquiring the lock.
            if os.path.exists(export_path):
                print(">>> Environment cache appeared while taking the lock.")
            else:
                return _extract_to_env_cache(export_path, extract_to)
        finally:
            _release_env_cache_lock(export_path, token)

    return _read_env_cache(export_path)


def _extract_to_env_cache(export_path, extract_to):
    """Run the extraction and publish its result as the shared cache file.

    Extraction writes to a path unique for this worker, so even a second
    worker that reclaimed an abandoned lock can't write into the same file.
    Only the final atomic replace makes the result visible to others.

    Args:
        export_path (str): Path to the shared cache file.
        extract_to (Callable[[str], None]): Runs the extraction process into
            the path it receives.

    Returns:
        dict[str, str]: Extracted environment variables, read back from the
            file this worker extracted. Publishing it for the other workers
            is best effort and never fails this worker.
    """
    temp_export_path = f"{export_path}.{_get_worker_id()}.tmp"
    try:
        print(
            f">>> '{export_path}' with extracted environment doesn't exist"
            " yet, running extraction process..."
        )
        extract_to(temp_export_path)
        if not os.path.exists(temp_export_path):
            raise RuntimeError(
                "Extraction process did not create expected file"
                f" '{temp_export_path}'."
            )
        contents = _load_environments(temp_export_path)

        print(f">>> Creating env var file {export_path}")
        try:
            # atomic for readers, they see either no file or the full one
            os.replace(temp_export_path, export_path)
        except OSError as exc:
            # e.g. Windows refuses to replace a file another worker reads
            print(f">>> Could not publish environment cache file: {exc}")
        return contents
    finally:
        _remove_silently(temp_export_path)


def _read_env_cache(export_path):
    """Read the shared cache file extracted by another worker.

    Args:
        export_path (str): Path to the shared cache file.

    Returns:
        Optional[dict[str, str]]: Environment variables or 'None' when the
            file is missing or unusable, so that the caller falls back to a
            private extraction instead of failing the job.
    """
    if not os.path.exists(export_path):
        return None

    print(f">>> Loading file '{export_path}' ...")
    try:
        return _load_environments(export_path)
    except (OSError, ValueError) as exc:
        print(f">>> Unable to use environment cache '{export_path}': {exc}")
        return None


def _extract_private_environments(extract_to):
    """Extract environments into a file used by this worker only.

    Args:
        extract_to (Callable[[str], None]): Runs the extraction process into
            the path it receives.

    Returns:
        dict[str, str]: Extracted environment variables.
    """
    temp_file_name = "ayon_env_{}_{}.json".format(
        datetime.utcnow().strftime("%Y%m%d%H%M%S%f"),
        uuid.uuid4().hex
    )
    export_path = os.path.join(tempfile.gettempdir(), temp_file_name)
    try:
        print(
            ">>> Running extraction process for this worker only into"
            f" '{export_path}'..."
        )
        extract_to(export_path)
        if not os.path.exists(export_path):
            raise RuntimeError(
                "Extraction process did not create expected file"
                f" '{export_path}'."
            )
        return _load_environments(export_path)
    finally:
        _remove_silently(export_path)


def _acquire_env_cache_lock(export_path, timeout):
    """Try to become the single worker that extracts the environments.

    Uses 'os.open' with 'O_CREAT | O_EXCL' which is atomic on local
    filesystems and on the NFS v4+ / SMB shares used by render farms, so only
    one worker of a job can create the lock file, all others wait for it.

    Waiting is deliberately short and never fatal. A worker that doesn't get
    the lock in time extracts the environment for itself instead of failing
    the job, which keeps a burst of dozens of workers starting at the same
    time functional even when the lock owner is slow or died on the way.

    A lock file older than 'ENV_CACHE_LOCK_STALE_TIMEOUT' seconds is treated
    as abandoned - a machine can lose power while holding it - and removed,
    so a job is not stuck behind it for the rest of its lifetime.

    Args:
        export_path (str): Path to the shared environment cache file. The
            lock file is 'export_path' with a '.lock' suffix.
        timeout (int): Seconds to wait for the current lock owner.

    Returns:
        tuple[str, Optional[str]]: Status and, when acquired, the token
            identifying our ownership of the lock file:
            - '_LOCK_ACQUIRED': run the extraction, then release the lock.
            - '_LOCK_CACHE_READY': the cache file is there, just read it.
            - '_LOCK_UNAVAILABLE': neither happened in time, extract into a
              worker private file instead.
    """
    lock_path = f"{export_path}.lock"
    token = _get_worker_id()
    give_up_at = time() + timeout
    waiting_logged = False

    while True:
        # Short-circuit, the final file appeared - nothing to extract.
        if os.path.exists(export_path):
            return _LOCK_CACHE_READY, None

        try:
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            pass  # another worker holds the lock, fall through to wait
        except OSError as exc:
            # locking is not possible at all, e.g. on a read only share
            print(f">>> Cannot use environment cache lock: {exc}")
            return _LOCK_UNAVAILABLE, None
        else:
            try:
                # 'fdopen' takes ownership of the descriptor and closes it,
                # also when writing the token fails
                with os.fdopen(fd, "w") as stream:
                    stream.write(token)
            except OSError as exc:
                # e.g. the share ran out of space or quota. Leaving the
                # lock file behind would block every other worker of this
                # job until it ages out, and there is no reason to fail
                # over it - extract into a private file instead.
                print(f">>> Cannot write environment cache lock: {exc}")
                _remove_silently(lock_path)
                return _LOCK_UNAVAILABLE, None
            print(f">>> Acquired extraction lock: {lock_path}")
            return _LOCK_ACQUIRED, token

        if _remove_abandoned_lock(lock_path):
            continue  # retry acquisition immediately

        if time() >= give_up_at:
            print(
                ">>> Another worker did not finish extracting the"
                f" environment within {timeout}s, extracting for this worker"
                " instead."
            )
            return _LOCK_UNAVAILABLE, None

        if not waiting_logged:
            print(
                ">>> Another worker is extracting the environment cache"
                f" file, waiting up to {timeout}s for: {export_path}"
            )
            waiting_logged = True

        sleep(ENV_CACHE_POLL_INTERVAL)


def _remove_abandoned_lock(lock_path):
    """Remove a lock file left behind by a worker that never came back.

    The threshold is much larger than the time any extraction should take,
    because the lock file's timestamp comes from the file server while the
    current time comes from the render node, and those two only agree as well
    as the farm's clock synchronization allows.

    Args:
        lock_path (str): Path to the lock file.

    Returns:
        bool: Whether an abandoned lock file was removed.
    """
    try:
        lock_age = time() - os.path.getmtime(lock_path)
    except OSError:
        return False  # lock disappeared, the caller retries anyway

    if lock_age < ENV_CACHE_LOCK_STALE_TIMEOUT:
        return False

    print(
        f">>> Lock '{lock_path}' looks abandoned ({lock_age:.0f}s old),"
        " removing it."
    )
    try:
        os.remove(lock_path)
    except OSError:
        return False  # another worker removed it first
    return True


def _release_env_cache_lock(export_path, token):
    """Release the extraction lock, but only while it is still ours.

    A lock considered abandoned may have been reclaimed by another worker
    while we were extracting. Removing that worker's lock would let a third
    one start extracting in parallel, so the token written when acquiring the
    lock is verified first.

    Args:
        export_path (str): Path to the shared environment cache file.
        token (str): Token returned by '_acquire_env_cache_lock'.
    """
    lock_path = f"{export_path}.lock"
    try:
        with open(lock_path) as stream:
            current_token = stream.read().strip()
    except OSError:
        return  # already removed by somebody else

    if current_token and current_token != token:
        print(
            f">>> Extraction lock '{lock_path}' was taken over by another"
            " worker, leaving it alone."
        )
        return

    try:
        os.remove(lock_path)
        print(f">>> Released extraction lock: {lock_path}")
    except OSError:
        pass


def _load_environments(path):
    """Read extracted environment variables from a JSON file.

    Args:
        path (str): Path to the JSON file.

    Returns:
        dict[str, str]: Environment variables.

    Raises:
        ValueError: If the file is not valid JSON or does not hold a mapping,
            e.g. because it was written by an older, non atomic version of
            this plugin and got truncated.
    """
    with open(path) as stream:
        contents = json.load(stream)

    if not isinstance(contents, dict):
        raise ValueError(
            f"File '{path}' does not contain environment variables."
        )
    return contents


def _remove_silently(path):
    """Remove a file if it exists, ignoring any failure to do so."""
    try:
        os.remove(path)
    except OSError:
        pass


def _get_worker_id():
    """Identifier unique for a single extraction attempt on this worker.

    Returns:
        str: Filename safe identifier, used both for the temporary file the
            extraction writes to and as the lock file's ownership token.
    """
    return "{}_{}_{}".format(
        re.sub(r"\W", "", platform.node()),
        os.getpid(),
        uuid.uuid4().hex[:8]
    )


def _get_output_dir(job):
    """Look for output dir where metadata.json should be created also."""
    output_urls = ["OutputFilePath", "Output", "SceneFile"]
    for output in output_urls:
        output_value = job.GetJobPluginInfoKeyValue(output)
        if not output_value:
            continue
        if os.path.isdir(output_value):
            return output_value
        if os.path.isfile(output_value):
            return os.path.dirname(output_value)
        # Path does not exist yet, guess if is file based on extension
        _, ext = os.path.splitext(output_value)
        if ext:
            return os.path.dirname(output_value)

    raise RuntimeError(
        "Unable to find workfile location or"
        " location where files should be rendered.")


def _extract_environments(
    ayon_server_url,
    ayon_api_key,
    ayon_studio_bundle_name,
    ayon_bundle_name,
    deadlinePlugin,
    exe,
    export_path,
    job
):
    """Calls `applications.extractenvironments` cli to get farm based envs."""
    print(f">>> Extracting environments to: {export_path}")

    add_kwargs = {
        "envgroup": "farm",
        "project": job.GetJobEnvironmentKeyValue("AYON_PROJECT_NAME"),
        "folder": job.GetJobEnvironmentKeyValue("AYON_FOLDER_PATH"),
        "task": job.GetJobEnvironmentKeyValue("AYON_TASK_NAME"),
        "app": job.GetJobEnvironmentKeyValue("AYON_APP_NAME"),
    }
    if not all(add_kwargs.values()):
        raise RuntimeError(
            "Missing required env vars: AYON_PROJECT_NAME,"
            " AYON_FOLDER_PATH, AYON_TASK_NAME, AYON_APP_NAME"
        )

    # Use applications addon arguments
    # TODO validate if applications addon should be used
    args = [
        "--headless",
        "addon",
        "applications",
        "extractenvironments",
        export_path
    ]

    for key, value in add_kwargs.items():
        args.extend([f"--{key}", value])

    environment = {
        "AYON_SERVER_URL": ayon_server_url,
        "AYON_API_KEY": ayon_api_key,
        "AYON_STUDIO_BUNDLE_NAME": ayon_studio_bundle_name,
        "AYON_BUNDLE_NAME": ayon_bundle_name,
    }

    for key in ("AYON_USE_STAGING", "AYON_IN_TESTS"):
        value = job.GetJobEnvironmentKeyValue(key)
        if value:
            environment[key] = value

    for env, val in environment.items():
        # Add the env var for the Render Plugin that is about to render
        deadlinePlugin.SetEnvironmentVariable(env, val)
        # Add the env var for current calls to `DeadlinePlugin.RunProcess`
        deadlinePlugin.SetProcessEnvironmentVariable(env, val)

    args_str = subprocess.list2cmdline(args)
    print(f">>> Executing: {exe} {args_str}")
    _process_exitcode = deadlinePlugin.RunProcess(
        exe, args_str, os.path.dirname(exe), -1
    )
    if _process_exitcode != 0:
        raise RuntimeError(
            "AYON process to extract environments"
            f" exited with error code: {_process_exitcode}"
        )


def get_ayon_executable():
    """Return AYON Executable from Event Plug-in Settings

    Returns:
        list[str]: AYON executable paths.

    Raises:
        RuntimeError: When no path configured at all.

    """
    config = RepositoryUtils.GetPluginConfig("Ayon")
    exe_list = config.GetConfigEntryWithDefault("AyonExecutable", "")

    if not exe_list:
        raise RuntimeError(
            "Path to AYON executable not configured."
            "Please set it in AYON Deadline Plugin."
        )

    # clean '\ ' for MacOS pasting
    if platform.system().lower() == "darwin":
        exe_list = exe_list.replace("\\ ", " ")

    # Expand user paths
    expanded_paths = []
    for path in exe_list.split(";"):
        if path.startswith("~"):
            path = os.path.expanduser(path)
        expanded_paths.append(path)
    return ";".join(expanded_paths)


def inject_render_job_id(deadlinePlugin):
    """Inject dependency ids to publish process as env var for validation."""
    print(">>> Injecting render job id ...")
    job = deadlinePlugin.GetJob()

    dependency_ids = job.JobDependencyIDs
    print(">>> Dependency IDs: {}".format(dependency_ids))
    render_job_ids = ",".join(dependency_ids)
    deadlinePlugin.SetProcessEnvironmentVariable(
        "RENDER_JOB_IDS", render_job_ids
    )

    ayon_server_url, ayon_api_key = handle_credentials(job)

    credentials = {
        "AYON_SERVER_URL": ayon_server_url,
        "AYON_API_KEY": ayon_api_key
    }
    for env, val in credentials.items():
        job.SetJobEnvironmentKeyValue(env, val)
    print(">>> Injection end.")


def handle_credentials(job):
    """Returns a tuple of values for AYON_SERVER_URL and AYON_API_KEY

    AYON_API_KEY might be overridden directly from job environments.
    Or specific AYON_SERVER_URL might be attached to job to pick corespondent
    AYON_API_KEY from plugin configuration.
    """
    config = RepositoryUtils.GetPluginConfig("Ayon")
    ayon_server_url = config.GetConfigEntryWithDefault("AyonServerUrl", "")
    ayon_api_key = config.GetConfigEntryWithDefault("AyonApiKey", "")

    job_ayon_server_url = job.GetJobEnvironmentKeyValue("AYON_SERVER_URL")
    job_ayon_api_key = job.GetJobEnvironmentKeyValue("AYON_API_KEY")

    # API key submitted with job environment will always take priority
    if job_ayon_api_key:
        ayon_api_key = job_ayon_api_key

    # Allow custom AYON API key per server URL if server URL is submitted
    # along with the job. The custom API keys can be configured on the
    # Deadline Repository AYON Plug-in settings, in the format of
    # `SERVER:PORT@APIKEY` per line.
    elif job_ayon_server_url and job_ayon_server_url != ayon_server_url:
        api_key = _get_ayon_api_key_from_additional_servers(
            config, job_ayon_server_url)
        if api_key:
            ayon_api_key = api_key
            print(">>> Using API key from Additional AYON Servers.")
        else:
            print(
                ">>> AYON Server URL submitted with job "
                f"'{job_ayon_server_url}' has no API key defined "
                "in AYON Deadline plugin configuration,"
                " `Additional AYON Servers` section."
                " Use Deadline monitor to modify the values."
                "Falling back to `AYON API key` set in `AYON Credentials`"
                " section of AYON plugin configuration."
            )
        ayon_server_url = job_ayon_server_url
    if not all([ayon_server_url, ayon_api_key]):
        raise RuntimeError(
            "Missing required values for server url and api key. "
            "Please fill in AYON Deadline plugin or provide by "
            "AYON_SERVER_URL and AYON_API_KEY"
        )
    return ayon_server_url, ayon_api_key


def _get_ayon_api_key_from_additional_servers(config, server):
    """Get AYON API key from the list of additional servers.

    The additional servers are configured on the DeadlineRepository AYON
    Plug-in settings using the `AyonAdditionalServerUrls` param. Each line
    represents a server URL with an API key, like:
        server1:port@APIKEY1
        server2:port@APIKEY2

    Returns:
        Optional[str]: If the server URL is found in the additional servers
            then return the API key for that server.

    """
    additional_servers: str = config.GetConfigEntryWithDefault(
        "AyonAdditionalServerUrls", "").strip()
    if not additional_servers:
        return

    if not isinstance(additional_servers, list):
        additional_servers = additional_servers.split(";")

    for line in additional_servers:
        line = line.strip()
        # Ignore empty lines
        if not line:
            continue

        # Log warning if additional server URL is misconfigured
        # without an API key
        if "@" not in line:
            print("Configured additional server URL lacks "
                  f"`@APIKEY` suffix: {line}")
            continue

        additional_server, api_key = line.split("@", 1)
        if additional_server == server:
            return api_key


def __main__(deadlinePlugin):
    print("*** GlobalJobPreload {} start ...".format(__version__))
    print(">>> Getting job ...")
    job = deadlinePlugin.GetJob()

    openpype_render_job = job.GetJobEnvironmentKeyValue(
        "OPENPYPE_RENDER_JOB")
    openpype_publish_job = job.GetJobEnvironmentKeyValue(
        "OPENPYPE_PUBLISH_JOB")
    openpype_remote_job = job.GetJobEnvironmentKeyValue(
        "OPENPYPE_REMOTE_PUBLISH")

    if openpype_publish_job == "1" and openpype_render_job == "1":
        raise RuntimeError(
            "Misconfiguration. Job couldn't be both render and publish."
        )

    if openpype_publish_job == "1":
        inject_render_job_id(deadlinePlugin)
    if openpype_render_job == "1" or openpype_remote_job == "1":
        inject_openpype_environment(deadlinePlugin)

    ayon_render_job = job.GetJobEnvironmentKeyValue("AYON_RENDER_JOB")
    ayon_publish_job = job.GetJobEnvironmentKeyValue("AYON_PUBLISH_JOB")
    ayon_remote_job = job.GetJobEnvironmentKeyValue("AYON_REMOTE_PUBLISH")

    if ayon_publish_job == "1" and ayon_render_job == "1":
        raise RuntimeError(
            "Misconfiguration. Job couldn't be both render and publish."
        )

    if ayon_publish_job == "1":
        inject_render_job_id(deadlinePlugin)
    if ayon_render_job == "1" or ayon_remote_job == "1":
        inject_ayon_environment(deadlinePlugin)
