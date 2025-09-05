#!/usr/bin/env python3

from System.IO import Path
from System.Text.RegularExpressions import Regex

from Deadline.Plugins import PluginType, DeadlinePlugin
from Deadline.Scripting import (
    StringUtils,
    FileUtils,
    RepositoryUtils
)

import re
import os
import platform

__version__ = "1.0.1"

######################################################################
# This is the function that Deadline calls to get an instance of the
# main DeadlinePlugin class.
######################################################################
def GetDeadlinePlugin():
    return AyonDeadlinePlugin()


def CleanupDeadlinePlugin(deadlinePlugin):
    deadlinePlugin.Cleanup()


class AyonDeadlinePlugin(DeadlinePlugin):
    """
        Standalone plugin for publishing from AYON

        Calls Ayonexecutable 'ayon_console' from first correctly found
        file based on plugin configuration. Uses 'publish' command and passes
        path to metadata json file, which contains all needed information
        for publish process.
    """
    def __init__(self):
        super().__init__()
        self.InitializeProcessCallback += self.InitializeProcess
        self.RenderExecutableCallback += self.RenderExecutable
        self.RenderArgumentCallback += self.RenderArgument

    def Cleanup(self):
        for stdoutHandler in self.StdoutHandlers:
            del stdoutHandler.HandleCallback

        del self.InitializeProcessCallback
        del self.RenderExecutableCallback
        del self.RenderArgumentCallback

    def InitializeProcess(self):
        self.LogInfo(
            "Initializing process with AYON plugin {}".format(__version__)
        )
        self.PluginType = PluginType.Simple
        self.StdoutHandling = True

        self.SingleFramesOnly = self.GetBooleanPluginInfoEntryWithDefault(
            "SingleFramesOnly", False)
        self.LogInfo("Single Frames Only: %s" % self.SingleFramesOnly)

        self.AddStdoutHandlerCallback(
            ".*Progress: (\\d+)%.*").HandleCallback += self.HandleProgress

    def RenderExecutable(self):
        job = self.GetJob()

        # set required env vars for AYON
        # cannot be in InitializeProcess as it is too soon
        ayon_server_url, ayon_api_key = handle_credentials(job)

        ayon_bundle_name = job.GetJobEnvironmentKeyValue("AYON_BUNDLE_NAME")

        environment = {
            "AYON_SERVER_URL": ayon_server_url,
            "AYON_API_KEY": ayon_api_key,
            "AYON_BUNDLE_NAME": ayon_bundle_name,
        }

        for env, val in environment.items():
            self.SetEnvironmentVariable(env, val)

        exe_list = self.GetConfigEntry("AyonExecutable")
        # clean '\ ' for MacOS pasting
        if platform.system().lower() == "darwin":
            exe_list = exe_list.replace("\\ ", " ")

        expanded_paths = []
        for path in exe_list.split(";"):
            if path.startswith("~"):
                path = os.path.expanduser(path)
            expanded_paths.append(path)
        exe = FileUtils.SearchFileList(";".join(expanded_paths))

        if exe == "":
            self.FailRender(
                "AYON executable was not found in the semicolon separated "
                "list: \"{}\". The path to the render executable can be "
                "configured from the Plugin Configuration in the Deadline "
                "Monitor.".format(exe_list)
            )
        return exe

    def RenderArgument(self):
        arguments = str(self.GetPluginInfoEntryWithDefault("Arguments", ""))
        arguments = RepositoryUtils.CheckPathMapping(arguments)

        arguments = re.sub(r"<(?i)STARTFRAME>", str(self.GetStartFrame()),
                           arguments)
        arguments = re.sub(r"<(?i)ENDFRAME>", str(self.GetEndFrame()),
                           arguments)
        arguments = re.sub(r"<(?i)QUOTE>", "\"", arguments)

        arguments = self.ReplacePaddedFrame(arguments,
                                            "<(?i)STARTFRAME%([0-9]+)>",
                                            self.GetStartFrame())
        arguments = self.ReplacePaddedFrame(arguments,
                                            "<(?i)ENDFRAME%([0-9]+)>",
                                            self.GetEndFrame())

        count = 0
        for filename in self.GetAuxiliaryFilenames():
            localAuxFile = Path.Combine(self.GetJobsDataDirectory(), filename)
            arguments = re.sub(r"<(?i)AUXFILE" + str(count) + r">",
                               localAuxFile.replace("\\", "/"), arguments)
            count += 1

        return arguments

    def ReplacePaddedFrame(self, arguments, pattern, frame):
        frameRegex = Regex(pattern)
        while True:
            frameMatch = frameRegex.Match(arguments)
            if not frameMatch.Success:
                break
            paddingSize = int(frameMatch.Groups[1].Value)
            if paddingSize > 0:
                padding = StringUtils.ToZeroPaddedString(
                    frame, paddingSize, False)
            else:
                padding = str(frame)
            arguments = arguments.replace(
                frameMatch.Groups[0].Value, padding)

        return arguments

    def HandleProgress(self):
        progress = float(self.GetRegexMatch(1))
        self.SetProgress(progress)


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
