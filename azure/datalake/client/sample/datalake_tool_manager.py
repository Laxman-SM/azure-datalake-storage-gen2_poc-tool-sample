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


class ListItemsResponse(object):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.items = []
        self.continuation_token = None

    def __str__(self):
        items_str = ', '.join(map(str, self.items))
        result_to_format = "'continuation_token': '{token}', 'items': [{items_str}]"
        result = "{" + result_to_format.format(token=self.continuation_token, items_str=items_str) + "}"

        return result


class DatalakeToolManager:
    def __init__(self, client, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client = client

    def create_filesystem(self, filesystem):
        self.client.filesystem.create(filesystem)

    def delete_filesystem(self, filesystem):
        self.client.filesystem.delete(filesystem)

    def list_filesystems(self, prefix=None, include_acl:bool=False, upn=None):
        filesystems_list = []
        list_response = self.client.filesystem.list(prefix=prefix, raw=True)
        for item in list_response:
            filesystems_dict = {}
            filesystems_dict['name'] = item.name
            filesystems_dict['last-modified'] = item.last_modified
            if include_acl:
                filesystem_acl = self.get_path_acl(item.name, upn=upn)
                filesystems_dict['acl'] = filesystem_acl
            filesystems_list.append(filesystems_dict)
        return filesystems_list

    def list_path_items(self, filesystem, path:str=None, recursive:bool=False, iterate_in_results:bool=False, max_results=None, upn=None):
        if path:
            filesystem_path = path
        else:
            filesystem_path = ''

        response_items = ListItemsResponse()

        iterate_more = True
        continuation_token = None
        while iterate_more:
            if continuation_token:
                list_response = self.client.path.list(recursive=recursive, filesystem=filesystem, directory=filesystem_path, max_results=max_results, upn=upn, continuation=continuation_token)
            else:
                list_response = self.client.path.list(recursive=recursive, filesystem=filesystem, directory=filesystem_path, max_results=max_results, upn=upn)
            
            for item in list_response:
                response_items.items.append(item)

            if 'x-ms-continuation' in list_response.raw.response.headers:
                continuation_token = list_response.raw.response.headers['x-ms-continuation']
            else:
                continuation_token = None

            if not iterate_in_results:
                iterate_more = False
                response_items.continuation_token = continuation_token
            elif not continuation_token:
                iterate_more = False
            elif max_results:
                iterate_more = (len(response_items.items) < max_results)
            else:
                iterate_more = True
        
        return response_items

    def create_folder(self, filesystem, folder):
        """
        :param client: the client
        :type client: ~azure.datalake.storage.v2018_11_09_poc.DataLakeStorageClient
        :param folder: the folder
        :type folder: str
        """
        self.client.path.create(filesystem, folder, PathResourceType.directory)

    def update_owner(self, filesystem, path, new_owner):
        self.client.path.update(PathUpdateAction.set_access_control, filesystem, path, x_ms_owner=new_owner)

    def update_group_owner(self, filesystem, path, new_group_owner):
        self.client.path.update(PathUpdateAction.set_access_control, filesystem, path, x_ms_group=new_group_owner)

    def _build_user_properties_dict(self, filesystem, path, response, decode_user_properties:bool=False):
        user_properties_header = response.headers['x-ms-properties']
        user_properties_dict = {}
        if user_properties_header:
            user_properties_split = user_properties_header.split(',')
            for user_property in user_properties_split:
                user_property_split = user_property.split('=')
                if decode_user_properties:
                    property_value_decoded = base64.b64decode( user_property_split[1])
                    user_property_value = property_value_decoded.decode("utf-8") 
                else:
                    user_property_value = user_property_split[1]

                user_properties_dict[user_property_split[0]] = user_property_value
        
        return user_properties_dict

    def get_path_user_properties(self, filesystem, path, decode_user_properties:bool=False):
        get_properties_response = self.client.path.get_properties(filesystem, path, action=None, raw=True)
        user_properties_dict = self._build_user_properties_dict(filesystem, path, get_properties_response, decode_user_properties)

        return user_properties_dict

    def _build_system_properties_dict(self, filesystem, path, response):
        system_properties_dict = {}
        system_properties_dict['Name'] = path
        system_properties_dict['URL'] = self.client.path.config.base_url.format(accountName=self.client.path.config.account_name, dnsSuffix=self.client.path.config.dns_suffix) + '/' + path
        system_properties_dict['Last-Modified'] = response.headers['Last-Modified']
        system_properties_dict['Cache-Control'] = response.headers['Cache-Control']
        system_properties_dict['Content-Type'] = response.headers['Content-Type']
        system_properties_dict['Content-Language'] = response.headers['Content-Language']
        system_properties_dict['Content-Disposition'] = response.headers['Content-Disposition']
        system_properties_dict['Owner'] = response.headers['x-ms-owner']
        system_properties_dict['Group'] = response.headers['x-ms-group']
        system_properties_dict['Permissions'] = response.headers['x-ms-permissions']
        system_properties_dict['ResourceType'] = response.headers['x-ms-resource-type']

        return system_properties_dict

    def get_path_system_properties(self, filesystem, path, upn=None):
        get_properties_response = self.client.path.get_properties(filesystem, path, action=None, raw=True, upn=upn)
        system_properties_dict = self._build_system_properties_dict(filesystem, path, get_properties_response)

        return system_properties_dict

    def get_path_properties(self, filesystem, path, decode_user_properties:bool=False, upn=None):
        get_properties_response = self.client.path.get_properties(filesystem, path, action=None, raw=True, upn=upn)
        properties_dict = {}

        user_properties_dict = self._build_user_properties_dict(filesystem, path, get_properties_response, decode_user_properties)
        properties_dict['user_properties'] = user_properties_dict

        system_properties_dict = self._build_system_properties_dict(filesystem, path, get_properties_response)
        properties_dict['system_properties'] = system_properties_dict

        return properties_dict

    def get_path_acl(self, filesystem, path=None, upn=None):
        if path:
            filesystem_path = path
        else:
            filesystem_path = ''
        get_properties_response = self.client.path.get_properties(filesystem, filesystem_path, PathGetPropertiesAction.get_access_control, raw=True, upn=upn)
        properties_acl = get_properties_response.headers['x-ms-acl']
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

    def download_file(self, filesystem, source_file_path, target_file_path):
        # var readStream = client.Path.Read(filesystem, dlFilePath);

        with open(target_file_path, "wb") as f:
            
            def user_callback(chunk, response):
                f.write(chunk)

            download_response =self.client.path.read(filesystem, source_file_path, raw=True, callback = user_callback)

            sync_iterator = download_response.output
            for value in sync_iterator:
                pass
