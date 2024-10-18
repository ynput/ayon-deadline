# -*- coding: utf-8 -*-
"""Abstract package for submitting jobs to Deadline.

It provides Deadline JobInfo data class.

"""
import json.decoder
from abc import abstractmethod
import getpass
import os
import datetime


import requests

import pyblish.api
from ayon_core.pipeline.publish import (
    AbstractMetaInstancePlugin,
    KnownPublishError,
    AYONPyblishPluginMixin
)
from ayon_core.pipeline.publish.lib import (
    replace_with_published_scene_path
)
from ayon_core.lib import is_in_tests

JSONDecodeError = getattr(json.decoder, "JSONDecodeError", ValueError)


def requests_post(*args, **kwargs):
    """Wrap request post method.

    Disabling SSL certificate validation if ``verify`` kwarg is set to False.
    This is useful when Deadline server is
    running with self-signed certificates and its certificate is not
    added to trusted certificates on client machines.

    Warning:
        Disabling SSL certificate validation is defeating one line
        of defense SSL is providing, and it is not recommended.

    """
    auth = kwargs.get("auth")
    if auth:
        kwargs["auth"] = tuple(auth)  # explicit cast to tuple
    # add 10sec timeout before bailing out
    kwargs['timeout'] = 10
    return requests.post(*args, **kwargs)


def requests_get(*args, **kwargs):
    """Wrap request get method.

    Disabling SSL certificate validation if ``verify`` kwarg is set to False.
    This is useful when Deadline server is
    running with self-signed certificates and its certificate is not
    added to trusted certificates on client machines.

    Warning:
        Disabling SSL certificate validation is defeating one line
        of defense SSL is providing, and it is not recommended.

    """
    auth = kwargs.get("auth")
    if auth:
        kwargs["auth"] = tuple(auth)
    # add 10sec timeout before bailing out
    kwargs['timeout'] = 10
    return requests.get(*args, **kwargs)


class AbstractSubmitDeadline(
    pyblish.api.InstancePlugin,
    AYONPyblishPluginMixin,
    metaclass=AbstractMetaInstancePlugin
):
    """Class abstracting access to Deadline."""

    label = "Submit to Deadline"
    order = pyblish.api.IntegratorOrder + 0.1

    import_reference = False
    use_published = True
    asset_dependencies = False
    default_priority = 50

    def __init__(self, *args, **kwargs):
        super(AbstractSubmitDeadline, self).__init__(*args, **kwargs)
        self._instance = None
        self._deadline_url = None
        self.scene_path = None
        self.job_info = None
        self.plugin_info = None
        self.aux_files = None

    def process(self, instance):
        """Plugin entry point."""
        self._instance = instance
        context = instance.context
        self._deadline_url = instance.data["deadline"]["url"]

        assert self._deadline_url, "Requires Deadline Webservice URL"

        file_path = None
        if self.use_published:
            if not self.import_reference:
                file_path = self.from_published_scene()
            else:
                self.log.info("use the scene with imported reference for rendering") # noqa
                file_path = context.data["currentFile"]

        # fallback if nothing was set
        if not file_path:
            self.log.warning("Falling back to workfile")
            file_path = context.data["currentFile"]

        self.scene_path = file_path
        self.log.info("Using {} for render/export.".format(file_path))

        job_info = self.get_generic_job_info(instance)
        self.job_info = self.get_job_info(job_info)
        self.plugin_info = self.get_plugin_info()
        self.aux_files = self.get_aux_files()

        job_id = self.process_submission()
        self.log.info("Submitted job to Deadline: {}.".format(job_id))

        # TODO: Find a way that's more generic and not render type specific
        if instance.data.get("splitRender"):
            self.log.info("Splitting export and render in two jobs")
            self.log.info("Export job id: %s", job_id)
            render_job_info = self.get_job_info(dependency_job_ids=[job_id])
            render_plugin_info = self.get_plugin_info(job_type="render")
            payload = self.assemble_payload(
                job_info=render_job_info,
                plugin_info=render_plugin_info
            )
            auth = instance.data["deadline"]["auth"]
            verify = instance.data["deadline"]["verify"]
            render_job_id = self.submit(payload, auth, verify)
            self.log.info("Render job id: %s", render_job_id)

    def process_submission(self):
        """Process data for submission.

        This takes Deadline JobInfo, PluginInfo, AuxFile, creates payload
        from them and submit it do Deadline.

        Returns:
            str: Deadline job ID

        """
        payload = self.assemble_payload()
        auth = self._instance.data["deadline"]["auth"]
        verify = self._instance.data["deadline"]["verify"]
        return self.submit(payload, auth, verify)

    def get_generic_job_info(self, instance):
        context = instance.context

        job_info = instance.data["deadline"]["job_info"]

        # Always use the original work file name for the Job name even when
        # rendering is done from the published Work File. The original work
        # file name is clearer because it can also have subversion strings,
        # etc. which are stripped for the published file.
        batch_name = os.path.basename(context.data["currentFile"])

        if is_in_tests():
            batch_name += datetime.now().strftime("%d%m%Y%H%M%S")

        job_info.Name = "%s - %s" % (batch_name, instance.name)
        job_info.BatchName = batch_name
        job_info.UserName = context.data.get("deadlineUser", getpass.getuser())  # TODO clean deadlineUser

        first_expected_file = instance.data["expectedFiles"][0]
        job_info.OutputFilename += os.path.basename(first_expected_file)
        job_info.OutputDirectory += os.path.dirname(first_expected_file)

        # Set job environment variables
        job_info.add_instance_job_env_vars(instance)
        job_info.add_render_job_env_var()

        return job_info

    @abstractmethod
    def get_job_info(self):
        """Return filled Deadline JobInfo.

        This is host/plugin specific implementation of how to fill data in.

        See:
            :class:`DeadlineJobInfo`

        Returns:
            :class:`DeadlineJobInfo`: Filled Deadline JobInfo.

        """
        pass

    @abstractmethod
    def get_plugin_info(self):
        """Return filled Deadline PluginInfo.

        This is host/plugin specific implementation of how to fill data in.

        See:
            :class:`DeadlineJobInfo`

        Returns:
            dict: Filled Deadline JobInfo.

        """
        pass

    def get_aux_files(self):
        """Return list of auxiliary files for Deadline job.

        If needed this should be overridden, otherwise return empty list as
        that field even empty must be present on Deadline submission.

        Returns:
            list: List of files.

        """
        return []

    def from_published_scene(self, replace_in_path=True):
        """Switch work scene for published scene.

        If rendering/exporting from published scenes is enabled, this will
        replace paths from working scene to published scene.

        Args:
            replace_in_path (bool): if True, it will try to find
                old scene name in path of expected files and replace it
                with name of published scene.

        Returns:
            str: Published scene path.
            None: if no published scene is found.

        Note:
            Published scene path is actually determined from project Anatomy
            as at the time this plugin is running scene can still no be
            published.

        """
        return replace_with_published_scene_path(
            self._instance, replace_in_path=replace_in_path)

    def assemble_payload(
            self, job_info=None, plugin_info=None, aux_files=None):
        """Assemble payload data from its various parts.

        Args:
            job_info (DeadlineJobInfo): Deadline JobInfo. You can use
                :class:`DeadlineJobInfo` for it.
            plugin_info (dict): Deadline PluginInfo. Plugin specific options.
            aux_files (list, optional): List of auxiliary file to submit with
                the job.

        Returns:
            dict: Deadline Payload.

        """
        job = job_info or self.job_info
        return {
            "JobInfo": job.serialize(),
            "PluginInfo": plugin_info or self.plugin_info,
            "AuxFiles": aux_files or self.aux_files
        }

    def submit(self, payload, auth, verify):
        """Submit payload to Deadline API end-point.

        This takes payload in the form of JSON file and POST it to
        Deadline jobs end-point.

        Args:
            payload (dict): dict to become json in deadline submission.
            auth (tuple): (username, password)
            verify (bool): verify SSL certificate if present

        Returns:
            str: resulting Deadline job id.

        Throws:
            KnownPublishError: if submission fails.

        """
        url = "{}/api/jobs".format(self._deadline_url)
        response = requests_post(
            url, json=payload, auth=auth, verify=verify)
        if not response.ok:
            self.log.error("Submission failed!")
            self.log.error(response.status_code)
            self.log.error(response.content)
            self.log.debug(payload)
            raise KnownPublishError(response.text)

        try:
            result = response.json()
        except JSONDecodeError:
            msg = "Broken response {}. ".format(response)
            msg += "Try restarting the Deadline Webservice."
            self.log.warning(msg, exc_info=True)
            raise KnownPublishError("Broken response from DL")

        # for submit publish job
        self._instance.data["deadlineSubmissionJob"] = result

        return result["_id"]
