import os
from azure.keyvault.secrets import SecretClient
from azure.identity import ClientSecretCredential

def get_key_vault_secret():
    """
        This function return the secret value of your key Vault.
    """
    key_vault_name = os.environ.get("KEY_VAULT_NAME", "")
    key_vault_url = f"https://{key_vault_name}.vault.azure.net"
    key_vault_secret_name = os.environ.get("KEY_VAULT_SECRET_NAME", "")
    credential = ClientSecretCredential(
        tenant_id=os.environ.get("KEY_VAULT_TENANT_ID", ""),
        client_id=os.environ.get("KEY_VAULT_CLIENT_ID", ""), 
        client_secret=os.environ.get("KEY_VAULT_CLIENT_SECRET", ""),)
    client = SecretClient(vault_url=key_vault_url, credential=credential)
    retrieved_secret = client.get_secret(key_vault_secret_name)

    return retrieved_secret.value
