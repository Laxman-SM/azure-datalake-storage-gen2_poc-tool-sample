import os

class DatalakeToolSettings:
    def __init__(self, account_name:str=None):
        self.account_name = account_name
    
    @staticmethod
    def from_environment():
        account_name = os.environ.get('ADL_TOOL_ACCOUNT_NAME')

        settings_instance = DatalakeToolSettings(account_name=account_name)
        return settings_instance

class DatalakeToolAuthSettings:
    def __init__(self, client_id:str=None, client_secret:str=None, tenant_id:str=None):
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id
    
    @staticmethod
    def from_environment():
        client_id = os.environ.get('ADL_TOOL_APP_CLIENTID')
        secret = os.environ.get('ADL_TOOL_APP_CLIENTSECRET')
        tenant_id = os.environ.get('ADL_TOOL_TENANTID')

        settings_instance = DatalakeToolAuthSettings(client_id=client_id,client_secret=secret,tenant_id=tenant_id)
        return settings_instance
