import os

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
