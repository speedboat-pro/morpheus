from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import WorkspaceObjectAccessControlRequest

w = WorkspaceClient()
#comfigure authorization
w.workspace.update_permissions(
    workspace_object_type="apps",
    workspace_object_id="my-analytics-app",
    access_control_list=[
        WorkspaceObjectAccessControlRequest(user_name="alice@example.com", permission_level="CAN_USE"),
        WorkspaceObjectAccessControlRequest(group_name="app-admins",       permission_level="CAN_MANAGE"),
    ],
)

# configure compute
w.api_client.do(
    "PATCH",
    f"/api/2.0/apps/my-analytics-app",
    body={"compute_size": "LARGE"}
)
