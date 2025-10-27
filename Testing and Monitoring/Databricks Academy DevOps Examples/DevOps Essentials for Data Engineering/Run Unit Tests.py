# Databricks notebook source
# MAGIC %md
# MAGIC ### Import Pytest

# COMMAND ----------

!pip install pytest==8.3.4

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Autoreload for Python modules
# MAGIC
# MAGIC If you are editing multiple files while developing Python code, you can enable the autoreload extension to reload any imported modules automatically so that command runs pick up those edits. Use the following commands in any notebook cell or Python file to enable the autoreload extension.
# MAGIC
# MAGIC Documentation: [Autoreload for Python modules](https://docs.databricks.com/en/files/workspace-modules.html#autoreload-for-python-modules)

# COMMAND ----------

## You can leave this commented out. We do not need this.
# %load_ext autoreload
# %autoreload 2

# COMMAND ----------

# MAGIC %md
# MAGIC ### Execute the Unit Tests
# MAGIC
# MAGIC **NOTE:** In the Vocareum lab environment you might see an Error in callback message, and it is safe to ignore since it does not affect your code or test results.

# COMMAND ----------

## import the `pytest` and `sys` modules.
import pytest
import sys

from src.helpers import project_functions

sys.dont_write_bytecode = True

retcode = pytest.main(["./tests/unit_tests/test_spark_helper_functions.py", "-v", "-p", "no:cacheprovider"])

# Fail the cell execution if there are any test failures.
assert retcode == 0, "The pytest invocation failed. See the log for details."
