"""
Train a scikit-learn model, convert it to ONNX, and log both to an MLflow run.

Requirements:
    pip install scikit-learn skl2onnx onnx onnxruntime mlflow

Run:
    python sklearn_to_onnx_mlflow.py
"""

import mlflow
import mlflow.sklearn
import mlflow.onnx
import numpy as np
import onnxruntime as rt
from skl2onnx import convert_sklearn
from skl2onnx.common.data_types import FloatTensorType
from sklearn.datasets import load_iris
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, f1_score
from sklearn.model_selection import train_test_split

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
EXPERIMENT_NAME = "sklearn-onnx-demo"
PARAMS = {
    "n_estimators": 200,
    "max_depth": 6,
    "random_state": 42,
}

# Point at your tracking server if you have one, otherwise defaults to ./mlruns
# mlflow.set_tracking_uri("http://localhost:5000")
mlflow.set_experiment(EXPERIMENT_NAME)

# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------
X, y = load_iris(return_X_y=True)
X = X.astype(np.float32)  # ONNX runtime prefers float32
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

with mlflow.start_run(run_name="rf-iris-onnx") as run:
    print(f"MLflow run_id: {run.info.run_id}")

    # -----------------------------------------------------------------------
    # 1. Train the sklearn model
    # -----------------------------------------------------------------------
    model = RandomForestClassifier(**PARAMS)
    model.fit(X_train, y_train)

    preds = model.predict(X_test)
    acc = accuracy_score(y_test, preds)
    f1 = f1_score(y_test, preds, average="macro")

    mlflow.log_params(PARAMS)
    mlflow.log_metrics({"accuracy": acc, "f1_macro": f1})
    print(f"accuracy={acc:.4f}  f1_macro={f1:.4f}")

    # -----------------------------------------------------------------------
    # 2. Convert to ONNX
    # -----------------------------------------------------------------------
    n_features = X_train.shape[1]
    initial_types = [("float_input", FloatTensorType([None, n_features]))]

    onnx_model = convert_sklearn(
        model,
        initial_types=initial_types,
        target_opset=17,
        # Return raw probability tensors instead of ZipMap dicts (easier downstream)
        options={id(model): {"zipmap": False}},
    )

    # -----------------------------------------------------------------------
    # 3. Sanity check: sklearn vs ONNX Runtime predictions must match
    # -----------------------------------------------------------------------
    sess = rt.InferenceSession(
        onnx_model.SerializeToString(), providers=["CPUExecutionProvider"]
    )
    input_name = sess.get_inputs()[0].name
    onnx_preds = sess.run(None, {input_name: X_test})[0]

    match_rate = float(np.mean(onnx_preds == preds))
    mlflow.log_metric("onnx_sklearn_prediction_match", match_rate)
    assert match_rate == 1.0, "ONNX predictions diverge from sklearn!"
    print(f"ONNX/sklearn prediction match: {match_rate:.0%}")

    # -----------------------------------------------------------------------
    # 4. Log both flavors to the run
    # -----------------------------------------------------------------------
    signature = mlflow.models.infer_signature(X_test, preds)

    mlflow.sklearn.log_model(
        sk_model=model,
        name="sklearn_model",
        signature=signature,
        input_example=X_test[:5],
    )

    mlflow.onnx.log_model(
        onnx_model=onnx_model,
        name="onnx_model",
        signature=signature,
        input_example=X_test[:5],
    )

    print("Logged artifacts: sklearn_model/, onnx_model/")
    print(f"View with: mlflow ui  →  experiment '{EXPERIMENT_NAME}'")

# ---------------------------------------------------------------------------
# Loading the ONNX model back from MLflow later:
#
#   logged = f"runs:/{run.info.run_id}/onnx_model"
#   pyfunc_model = mlflow.pyfunc.load_model(logged)
#   pyfunc_model.predict(X_test)          # via pyfunc wrapper
#
#   # or load the raw ONNX proto:
#   onnx_proto = mlflow.onnx.load_model(logged)
# ---------------------------------------------------------------------------
