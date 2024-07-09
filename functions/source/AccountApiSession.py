from BaseDatabricksApiSession import BaseDatabricksApiSession
from WorkspaceApiSession import WorkspaceApiSession
from urllib3.util import make_headers
import requests


def get_access_token(clientId, clientSecret, accountId) : 
  token_url = f"https://accounts.cloud.databricks.com/oidc/accounts/{accountId}/v1/token"
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
class AccountApiSession(BaseDatabricksApiSession):

  # Initializes the session with the account id, user name and password
  def __init__(self, accountId: str, clientId: str, clientSecret: str, userAgent: str = None):
    # obtain access token
    accessToken = get_access_token(clientId, clientSecret, accountId)

    baseURL = f'https://accounts.cloud.databricks.com/api/2.0/accounts/{accountId}'
    headers = {
      'Authorization': f'Bearer {accessToken}',
      'Content-Type': 'application/json'
    }
    self.__userAgent = userAgent
    if userAgent is not None:
      headers['User-Agent'] = userAgent
    super().__init__(baseURL, headers)

  # reates a session for a deployed workspace using the same credentials
  def workspaceApiSession(self, deploymentName: str):
    return WorkspaceApiSession(deploymentName, self.__clientId, self.__clientSecret, self.__userAgent)
