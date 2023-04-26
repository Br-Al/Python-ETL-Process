# import environ
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.user_credential import UserCredential
from office365.runtime.auth.client_credential import ClientCredential
from office365.sharepoint.files.file import File
from common.config import get_api_config
import datetime
import pandas as pd
import io


def authorize(SHAREPOINT_SITE,USERNAME,PASSWORD):
    """
        Authenticate to the organization sharepoint.

        Parameters
        ----------
        SHAREPOINT_SITE: string
            The url of a sharepoint site.
        USERNAME : string
            The username of an organization account.
        PASSWORD : string
            The secret password of the organization account.
        Returns
        -------
        Access to the sharepoint data: Tables.

        """

    conn = ClientContext(SHAREPOINT_SITE).with_credentials(
        UserCredential(
            USERNAME,
            PASSWORD
        )
    )
    return conn

def download_file(authorize_data,SHAREPOINT_SITE_NAME,SHAREPOINT_DOC,folder_name, file_name):
    """
            Download the file in sharepoint.

            Parameters
            ----------
            authorize_data: boolean
                The access to a sharepoint .
            SHAREPOINT_SITE_NAME : string
                The name of a sharepoint site.
            SHAREPOINT_DOC : string
                The shared document.
            folder_name : string
                The name of the folder .
            file_name : string
                The name of a file to be downloaded.
            Returns
            -------
            sharepoint data: Tables.

            """

    file_url = f'/sites/{SHAREPOINT_SITE_NAME}/{SHAREPOINT_DOC}/{folder_name}/{file_name}'
    response = File.open_binary(authorize_data, file_url)

    bytes_file_obj = io.BytesIO()
    bytes_file_obj.write(response.content)
    bytes_file_obj.seek(0)
    return bytes_file_obj


