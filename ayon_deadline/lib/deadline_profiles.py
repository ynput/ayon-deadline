import json
import os
from ayon_core.lib import Logger
from ayon_core.pipeline import KnownPublishError
from ayon_core.settings import get_project_settings


class DeadlineProfilesMixin:
    """Mixin to fetch and match Deadline profiles."""

    def find_matching_profile(self, project_name, task_entity,
                              folder_path, task_name, host_name,
                              job_type="render"):
        """Find first profile matching the given criteria.

        Args:
            project_name (str): Project name.
            task_entity (dict): Task entity data.
            folder_path (str): Folder path.
            task_name (str): Task name.
            host_name (str): Host application name.
            job_type (str): Type of job (render, publish, caches).

        Returns:
            dict: Profile settings or None.
        """
        settings = get_project_settings(project_name)
        profiles = settings.get("deadline", {}).get("profiles", [])

        for profile in profiles:
            # Check job_type filter
            profile_job_types = profile.get("job_types", [])
            if profile_job_types and job_type not in profile_job_types:
                continue

            # Check other filters (task, folder, host, etc.)
            if self._match_profile_filters(profile, task_entity,
                                           folder_path, task_name,
                                           host_name):
                return profile.get("settings", {})

        return None

    def _match_profile_filters(self, profile, task_entity,
                               folder_path, task_name, host_name):
        """Check if profile filters match the context.

        Simplified implementation; actual matching logic may vary.
        """
        # Placeholder: return True if no filters set or all match
        filters = profile.get("filters", {})

        # Example: filter by task type
        task_types = filters.get("task_types", [])
        if task_types:
            task_type = task_entity.get("type")
            if task_type not in task_types:
                return False

        # Filter by host
        hosts = filters.get("hosts", [])
        if hosts and host_name not in hosts:
            return False

        # Filter by folder path (regex)
        folder_filters = filters.get("folder_paths", [])
        if folder_filters:
            import re
            if not any(re.match(p, folder_path) for p in folder_filters):
                return False

        return True
