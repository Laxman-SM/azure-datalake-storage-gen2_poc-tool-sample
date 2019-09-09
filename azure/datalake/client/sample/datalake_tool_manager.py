import os
import io
import base64


#TODO: select version by param
from azure.datalake.storage.v2018_11_09_poc import DataLakeStorageClient
from azure.datalake.storage.v2018_11_09_poc import VERSION
from msrestazure.azure_active_directory import AADTokenCredentials
from azure.identity import ClientSecretCredential, ManagedIdentityCredential, EnvironmentCredential

from ._constants import ManagerConstants

from azure.datalake.storage.v2018_11_09_poc.models import *

class DatalakeClientFactory:
    @staticmethod
    def _create_client(credential, account_name) -> DataLakeStorageClient:
        scopes = "https://storage.azure.com/.default"
        token = credential.get_token(scopes)
        token_dict = token._asdict()
        token_dict['access_token']=token_dict['token']
        aad_token_credentials = AADTokenCredentials(token_dict)

        dns_suffix = "dfs.core.windows.net"

        client = DataLakeStorageClient(aad_token_credentials, account_name, dns_suffix, x_ms_version=VERSION)
        client.config.base_url = 'https://{accountName}.{dnsSuffix}'
        return client
    
    @staticmethod
    def create_client_from_environment(account_name) -> DataLakeStorageClient:
        credential = EnvironmentCredential()
        return DatalakeClientFactory._create_client(credential, account_name)

    @staticmethod
    def create_client_from_managed_identity(account_name) -> DataLakeStorageClient:
        credential = ManagedIdentityCredential()
        return DatalakeClientFactory._create_client(credential, account_name)

    @staticmethod
    def create_client_from_service_principal(account_name, client_id, client_secret, tenant_id) -> DataLakeStorageClient:
        # Build credentials for datalake client
        credential = ClientSecretCredential(client_id, client_secret, tenant_id)
        return DatalakeClientFactory._create_client(credential, account_name)


class DatalakeToolManager:
    def __init__(self, client, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client = client

    def create_filesystem(self, filesystem):
        self.client.filesystem.create(filesystem)

    def delete_filesystem(self, filesystem):
        self.client.filesystem.delete(filesystem)

    def list_filesystem(self, filesystem):
        #TODO
        pass

    def create_folder(self, filesystem, folder):
        """
        :param client: the client
        :type client: ~azure.datalake.storage.v2018_11_09_poc.DataLakeStorageClient
        :param folder: the folder
        :type folder: str
        """
        self.client.path.create(filesystem, folder, PathResourceType.directory)

    def get_path_acl(self, filesystem, folder):
        properties = self.client.path.get_properties(filesystem, folder, PathGetPropertiesAction.get_access_control, raw=True)
        properties_acl = properties.headers['x-ms-acl']
        return properties_acl

    def update_owner(self, filesystem, path, new_owner):
        self.client.path.update(PathUpdateAction.set_access_control, filesystem, path, x_ms_owner=new_owner)

    def update_group_owner(self, filesystem, path, new_group_owner):
        self.client.path.update(PathUpdateAction.set_access_control, filesystem, path, x_ms_group=new_group_owner)

    def get_path_properties(self, filesystem, path):
        properties = self.client.path.get_properties(filesystem, path, action=None, raw=True)
        user_properties = properties.headers['x-ms-properties']
        properties_dict = {}
        if user_properties:
            user_properties_split = user_properties.split(',')
            for user_property in user_properties_split:
                user_property_split = user_property.split('=')
                properties_dict[user_property_split[0]] = user_property_split[1]

            return properties_dict

    def get_path_properties_decoded(self, filesystem, path):
        properties = self.client.path.get_properties(filesystem, path, action=None, raw=True)
        user_properties = properties.headers['x-ms-properties']
        user_properties_split = user_properties.split(',')
        decoded_properties_dict = {}
        for user_property in user_properties_split:
            user_property_split = user_property.split('=')
            property_value_decoded = base64.b64decode(user_property_split[1])
            decoded_properties_dict[user_property_split[0]] = property_value_decoded

        return decoded_properties_dict

    def get_path_acl(self, filesystem, path):
        access_properties = self.client.path.get_properties(filesystem, path, PathGetPropertiesAction.get_access_control, raw=True)
        properties_acl = access_properties.headers['x-ms-acl']
        return properties_acl

    def update_path_acl(self, filesystem, path, new_acl):
        self.client.path.update(PathUpdateAction.set_access_control, filesystem, path, x_ms_acl=new_acl);

    def upload_file(self, filesystem, source_file_path, target_folder):
        file_name = os.path.basename(source_file_path)
        datalake_filepath = target_folder + "/" + file_name

        chunk_size = ManagerConstants.CHUNK_SIZE_DEFAULT
        #chunk_size = 12 #for testing purpose
        
        position = 0
        with open(source_file_path, "rb") as f:
            self.client.path.create(filesystem, datalake_filepath, PathResourceType.file);

            # send using chunks
            while True:
                buffer = f.read(chunk_size)
                num_bytes_read = len(buffer)
                if (not num_bytes_read > 0):
                    break
                
                with io.BytesIO(buffer) as mem_file:
                    #Invoke upload
                    self.client.path.update(PathUpdateAction.append, filesystem, datalake_filepath, position=position, content_length=num_bytes_read, request_body=mem_file);

                position = position + num_bytes_read

            self.client.path.update(PathUpdateAction.flush, filesystem, datalake_filepath, position=position);

            # # using whole file approach
            # # file_size = <>
            # # with open(file_path, "rb") as f:
            # #     manager.client.path.create(filesystem, datalake_filepath, PathResourceType.file);
            # #     manager.client.path.update(PathUpdateAction.append, filesystem, datalake_filepath, position=position, content_length=file_size, request_body=f);
            # #     manager.client.path.update(PathUpdateAction.flush, filesystem, datalake_filepath, position=file_size);
