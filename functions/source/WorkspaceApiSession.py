from BaseDatabricksApiSession import BaseDatabricksApiSession
from urllib3.util import make_headers
import requests


def get_access_token(clientId, clientSecret, workspaceDeploymentName) : 
  token_url = f"https://${workspaceDeploymentName}/oidc/v1/token"
  response = requests.post(
    token_url, 
    auth=(clientId, clientSecret),
    data={
      'grant_type': 'client_credentials',
      'scope': 'all_api'
    } 
   )

  response.raise_for_status() # raise an error for bad status codes
  return response.json()['access_token']

# Class handling the calls to the Databricks Account REST API
class WorkspaceApiSession(BaseDatabricksApiSession):

  # Initializes the session with the workspace deployment name, user name and password
  def __init__(self, workspaceDeploymentName: str, clientId: str, clientSecret: str, userAgent: str = None):
    accessToken = get_access_token(clientId, clientSecret, workspaceDeploymentName)

    baseURL = 'https://{workspaceDeploymentName}.cloud.databricks.com/api/2.0'
    headers = {
      'Authorization': f'Bearer {accessToken}',
      'Content-Type': 'application/json'
    }
    if userAgent is not None:
      headers['User-Agent'] = userAgent
    super().__init__(baseURL, headers)
