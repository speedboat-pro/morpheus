# pip install databricks-sdk
from databricks.sdk import WorkspaceClient
from time import sleep

APP_NAME = "my-app"  # lowercase/hyphens per Apps naming
w = WorkspaceClient()  # uses your Databricks auth context

def status(name: str):
    app = w.apps.get(name=name)
    st = (app.active_deployment.status.state if app.active_deployment and app.active_deployment.status else "UNKNOWN")
    msg = (app.active_deployment.status.message if app.active_deployment and app.active_deployment.status else "")
    return st, msg

def wait_until(name: str, wanted: set[str], timeout_s=600, poll_s=5):
    t = 0
    while t <= timeout_s:
        st, msg = status(name)
        print(f"[{t:>3}s] {name} -> {st} {('- ' + msg) if msg else ''}")
        if st in wanted: return st
        sleep(poll_s); t += poll_s
    raise TimeoutError(f"Timed out waiting for {wanted}")

# Example: ensure app is RUNNING
w.apps.start(name=APP_NAME)         # start call
wait_until(APP_NAME, {"RUNNING"})   # poll until running

# Later: stop it
w.apps.stop(name=APP_NAME)
wait_until(APP_NAME, {"STOPPED"})
