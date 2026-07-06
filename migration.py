import json

def migrate_old_overrides(project_settings):
    """Migrate old plain attribute overrides to job-type-prefixed versions."""
    # Assumes project_settings contains old keys like "Priority" which should become "render_Priority"
    # This would be run once on project load
    pass
