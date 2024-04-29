from typing import Optional
import xarray as xr
import time
import os
import logging
logger = logging.getLogger(__name__)

try:
    from ._version import __version__
except ImportError:
    __version__ = "unknown"

cred_cache = {}

# HACK: patch xarray to have a call hook for lazy/eager operations
def assume_role():
    import boto3
    import botocore

    assume_role_credential_kwargs = {
        "RoleArn": "arn:aws:iam::444055461661:role/veda-data-reader-dev",
        "RoleSessionName": "emr-pforge-runner",
        "DurationSeconds": 3600,
    }

    client = boto3.client('sts')

    try:
        payload = client.assume_role(**assume_role_credential_kwargs)
        creds = payload['Credentials']
        os.environ['AWS_ACCESS_KEY_ID'] = creds['AccessKeyId']
        os.environ['AWS_SECRET_ACCESS_KEY'] = creds['SecretAccessKey']
        os.environ['AWS_SESSION_TOKEN'] = creds['SessionToken']
        return {
            'key': creds['AccessKeyId'],
            'secret': creds['SecretAccessKey'],
            'token': creds['SessionToken'],
            'anon': False,
        }
    except (botocore.exceptions.ClientError, botocore.exceptions.ParamValidationError) as exc:
        logger.exception(exc)


def cache_credentials(cred_payload, expiry_seconds):
    cred_cache["creds"] = (cred_payload, time.time() + expiry_seconds)


def get_credentials() -> Optional[tuple]:
    return cred_cache.get("creds", None)


def delete_credentials():
    try:
        del cred_cache["creds"]
    except KeyError:
        pass


def refresh_iamrole_on_getattribute(self, name):
    logger.info(f"intercept '__getattribute__' for lookup '{name}'")

    cached_creds = get_credentials()
    if cached_creds:
        cred_payload, cached_expiry_seconds = cached_creds

        if time.time() < cached_expiry_seconds:
            # credentials already set in os env vars
            #return cred_payload
            return self.original_getattribute(name)
        else:
            delete_credentials()

    # given the restrictions of assume role chaining
    # we're sticking with an hour expiry minus 30 secs
    # just so we actually never hit the expiry
    expiry_seconds = (60 * 60) - 30

    cred_payload = assume_role()
    cache_credentials(cred_payload, expiry_seconds)
    # credentials already set in os env vars
    #return cred_payload
    return self.original_getattribute(name)

xr.Dataset.original_getattribute = xr.Dataset.__getattribute__
xr.Dataset.__getattribute__ = refresh_iamrole_on_getattribute