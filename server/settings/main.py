from typing import TYPE_CHECKING
from pydantic import validator

from ayon_server.settings import (
    BaseSettingsModel,
    SettingsField,
    ensure_unique_names,
)
if TYPE_CHECKING:
    from ayon_server.addons import BaseServerAddon

from .publish_plugins import (
    PublishPluginsModel,
    DEFAULT_DEADLINE_PLUGINS_SETTINGS
)


async def defined_deadline_ws_name_enum_resolver(
    addon: "BaseServerAddon",
    settings_variant: str = None,
) -> list[str]:
    """Provides list of names of configured Deadline webservice urls."""
    if addon is None:
        return []

    settings = await addon.get_studio_settings(variant=settings_variant)

    ws_server_name = []
    for deadline_url_item in settings.deadline_urls:
        ws_server_name.append(deadline_url_item.name)

    return ws_server_name

class ServerItemSubmodel(BaseSettingsModel):
    """Connection info about configured DL servers."""
    _layout = "expanded"
    name: str = SettingsField(title="Name")
    value: str = SettingsField(title="Url")
    require_authentication: bool = SettingsField(
        False, title="Require authentication")
    not_verify_ssl: bool = SettingsField(
        False, title="Don't verify SSL")
    default_username: str = SettingsField(
        "",
        title="Default user name",
        description="Webservice username, 'Require authentication' must be "
                    "enabled."
    )
    default_password: str = SettingsField(
        "",
        title="Default password",
        description="Webservice password, 'Require authentication' must be "
                    "enabled."
    )


class DeadlineSettings(BaseSettingsModel):
    # configured DL servers
    deadline_urls: list[ServerItemSubmodel] = SettingsField(
        default_factory=list,
        title="System Deadline Webservice Info",
        scope=["studio"],
    )

    # name(key) of selected server for project
    deadline_server: str = SettingsField(
        title="Selected Deadline server name",
        section="---",
        scope=["project", "site"],
        enum_resolver=defined_deadline_ws_name_enum_resolver,
        description="Select one from predefined Deadline servers from Studio "
                    "Settings to be used for this Project"
    )

    publish: PublishPluginsModel = SettingsField(
        default_factory=PublishPluginsModel,
        title="Publish Plugins",
    )

    @validator("deadline_urls")
    def validate_unique_names(cls, value):
        ensure_unique_names(value)
        return value



DEFAULT_VALUES = {
    "deadline_urls": [
        {
            "name": "default",
            "value": "http://127.0.0.1:8082",
            "require_authentication": False,
            "not_verify_ssl": False,
            "default_username": "",
            "default_password": ""

        }
    ],
    "deadline_server": "default",
    "publish": DEFAULT_DEADLINE_PLUGINS_SETTINGS
}
