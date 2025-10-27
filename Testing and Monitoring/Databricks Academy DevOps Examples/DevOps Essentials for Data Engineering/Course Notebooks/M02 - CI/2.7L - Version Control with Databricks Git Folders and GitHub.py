# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img
# MAGIC     src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png"
# MAGIC     alt="Databricks Learning"
# MAGIC   >
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # 2.7 Lab - Version Control with Databricks Git Folders and GitHub
# MAGIC
# MAGIC
# MAGIC ### NOTE: This lab requires PERSONAL GitHub account to complete
# MAGIC
# MAGIC ### Estimated Duration: 25-30 minutes
# MAGIC
# MAGIC This lab requires a PERSONAL GitHub account. Complete this lab if you would like to practice using GitHub and Databricks. It will cover the basics of working with Databricks and GitHub, providing a simple introduction to Git folders and GitHub.
# MAGIC
# MAGIC ## Overview 
# MAGIC Git is an open source framework, but we will be utilizing a Git provider called [GitHub](https://github.com/). This is where your codebase will be hosted for development that:
# MAGIC - Hosts Git repos 
# MAGIC - Provides mechanism for access control 
# MAGIC - Provides tools for merge requests 
# MAGIC - Provides tools and features for CI/CD Pipeline testing ([GitHub Actions](https://docs.databricks.com/en/dev-tools/bundles/ci-cd-bundles.html))
# MAGIC
# MAGIC
# MAGIC ### Learning Objectives
# MAGIC By the end of this course, you will be able to: 
# MAGIC - Connect your GitHub account to Databricks Git folders using a Personal Access Token (PAT).
# MAGIC - Perform basic Git operations like push, commit, pull in Databricks and Merge within GitHub.
# MAGIC
# MAGIC **NOTE:** This course does not cover GitHub Actions. Please see the link provided above for further reading on this topic.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## A. Create a GitHub Account
# MAGIC If you already have a **non-work related GitHub account**, please go ahead and login. 
# MAGIC
# MAGIC Otherwise, you will need to follow the instructions provided [here](https://docs.github.com/en/get-started/start-your-journey/creating-an-account-on-github) to create your own personal GitHub account. 
# MAGIC
# MAGIC ### 🚨 Do not use a work related GitHub account 🚨

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Create a GitHub Repository
# MAGIC To create a GitHub repository (repo), follow the steps laid out [here](https://docs.github.com/en/repositories/creating-and-managing-repositories/quickstart-for-repositories#create-a-repository).
# MAGIC
# MAGIC When creating your GitHub repo, make sure the following are set:
# MAGIC
# MAGIC 1. Name the repo **databricks_devops**.
# MAGIC
# MAGIC 2. Do not add a **README** file.
# MAGIC
# MAGIC 3. Do not add a **gitignore** file.
# MAGIC
# MAGIC 4. Do not add a license.
# MAGIC
# MAGIC 5. After you create a GitHub repo you will get a note that looks like the following: ![GitHub](../Includes/images/05_github_new_repo.png)
# MAGIC
# MAGIC 6. Select **HTTPS** and leave the page open. You will need the HTTP link to create your Git folder in Databricks.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Setting up GitHub with Databricks
# MAGIC
# MAGIC Here we will integrate the GitHub repository you just created on GitHub with Databricks using Databricks Git Folders. 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### C1. Connect to a GitHub Repo Using a Personal Access Token (PAT)
# MAGIC
# MAGIC Either of the following approaches will work for this demonstration.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Option 1 (Recommended): Fine-Grained PAT Configuration
# MAGIC
# MAGIC For fine-grained access, you should connect to the repository using a fine-grained PAT. [Connect to a GitHub repo using a fine-grained personal access token](https://docs.databricks.com/en/repos/get-access-tokens-from-git-provider.html#connect-to-a-github-repo-using-a-fine-grained-personal-access-token) for details. 
# MAGIC
# MAGIC Here is a table that summarizes the permission and access level you will need when configuring your fine-grained PAT. 
# MAGIC | Permission      | Access Level      |
# MAGIC |---------------|----------------|
# MAGIC | Administration | Read and Write |
# MAGIC | Contents      | Read and Write |
# MAGIC | Pull Requests | Read and Write |
# MAGIC | Webhooks      | Read and Write |
# MAGIC
# MAGIC Be aware that Metadata will automatically be turned to read-only as a mandatory configuration. 
# MAGIC
# MAGIC **NOTE:** Select an custom expiration of tomorrow since you are using the Databricks Academy lab. However, the PAT will be removed when the lab is closed.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Option 2: Legacy PAT Configuration
# MAGIC
# MAGIC Follow the steps outlined in [Connect to a GitHub repo using a personal access token](https://docs.databricks.com/en/repos/get-access-tokens-from-git-provider.html#connect-to-a-github-repo-using-a-personal-access-token) for connecting to GitHub via a personal access token (PAT) with broad access to resources. 
# MAGIC
# MAGIC Here is a table that summarizes the permission and access level you will need if you need to configure a Legacy PAT. 
# MAGIC | Permission        | Access Level             |
# MAGIC |------------------|-------------------------|
# MAGIC | Repo            | Full control of private repositories |
# MAGIC | Workflow        | Read and Write access to workflows |
# MAGIC | Admin:Repo_Hook | Manage repository hooks |
# MAGIC | Delete_Repo     | Delete repositories |
# MAGIC
# MAGIC **NOTE:** Select an custom expiration of tomorrow since you are using the Databricks Academy lab. However, the PAT will be removed when the lab is closed.

# COMMAND ----------

# MAGIC %md
# MAGIC ### C2. Create a Git Folder in Databricks
# MAGIC
# MAGIC Next, we're going to connect to the GitHub repo you created in a previous section.
# MAGIC
# MAGIC Git folders in Databricks (formerly known as Repos) are used for version control and are enabled by default in your Databricks workspace. After setting up a Git folder, you can perform common Git operations such as clone, checkout, commit, push, pull, and manage branches directly from the Databricks UI.
# MAGIC
# MAGIC Complete the following:
# MAGIC
# MAGIC 1. In your GitHub account, navigate to the repository you created earlier and copy the HTTPS address. It should look like: `https://github.com/<github_username>/<repo_name>.git`.
# MAGIC    
# MAGIC 2. In Databricks within a new tab, go to **Workspace**, click on **Users**, and find your username for this course.
# MAGIC
# MAGIC 3. Click the blue **Create** button in the top-right corner.
# MAGIC
# MAGIC 4. Select **Git folder** from the dropdown.
# MAGIC
# MAGIC 5. Paste the copied URL into the textbox under **Git repository URL**.
# MAGIC
# MAGIC 6. Click the blue button at the bottom labeled **Create Git folder**.
# MAGIC
# MAGIC 7. In the Workspace, select your **databricks_devops** folder.
# MAGIC
# MAGIC 8. To the right of your folder name, you should see the **main** branch listed.
# MAGIC
# MAGIC
# MAGIC **Git Folder**
# MAGIC
# MAGIC ![Git Folder](../Includes/images/05_devops_db_git_folder.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Your First Push and Commit
# MAGIC
# MAGIC Let's add a file to your **Databricks Git folder** within the Databricks Workspace, then push and commit the changes to GitHub.
# MAGIC
# MAGIC 1. From Databricks, navigate to your **Git folder in your Workspace**, and in that folder create a file named **README.md**.
# MAGIC
# MAGIC 2. Add the following text to the **README.md** file:
# MAGIC     ```
# MAGIC     # Databricks DevOps Training
# MAGIC     My first push and commit.
# MAGIC     ```
# MAGIC
# MAGIC 3. At the top of the file, next to the file name **README.md**, you’ll see your repo branch name, **main**.
# MAGIC
# MAGIC 4. Select the **main** branch (your only branch). You should see the following:
# MAGIC
# MAGIC ![Changes](../Includes/images/05_first_push.png)
# MAGIC
# MAGIC **NOTE:** This page lets you see the branch you're working on, what changes have been made, settings, and gives you the option to commit and push changes to your GitHub repo.
# MAGIC
# MAGIC 5. We want to commit and push the new **README.md** file that we created in the Databricks Git folder. Enter a commit message (e.g., "First commit") and click **Commit & Push**.
# MAGIC
# MAGIC 6. Once you receive a confirmation message that the commit and push were successful, close the Git pop-up.
# MAGIC
# MAGIC 7. Go to your GitHub repo and refresh the page.
# MAGIC
# MAGIC 8. Select the **Code** tab at the top (if you're not already there). Here, you’ll find your newly uploaded file along with the commit message.

# COMMAND ----------

# MAGIC %md
# MAGIC #### D1. Creating a New Branch
# MAGIC
# MAGIC Let's create a **dev** branch within the **Databricks Git folder** to develop a new feature, and demonstrate how a pull request in GitHub reflects in our Git folder.
# MAGIC
# MAGIC Complete the following:
# MAGIC
# MAGIC 1. In the Databricks Workspace, select the **main** branch of your folder. In the Databricks Git folder popup, click **Create Branch**, name the branch **dev**, and then click **Create**.
# MAGIC
# MAGIC 2. Close the Databricks Git folder popup.
# MAGIC
# MAGIC 3. You should now be in the **dev** branch.
# MAGIC
# MAGIC **NOTE:** Ensure that **dev** is selected by default, as we will be committing to this branch first. At this point, the **dev** and **main** branches are identical.

# COMMAND ----------

# MAGIC %md
# MAGIC #### D2. Add a Feature to the Dev Branch
# MAGIC
# MAGIC Let's pretend we are creating a new feature in our **dev** branch.
# MAGIC
# MAGIC Complete the following:
# MAGIC
# MAGIC 1. Go to your Git folder in the Databricks Workspace and ensure the **dev** branch is selected.
# MAGIC
# MAGIC 2. Within the **dev** branch, create a notebook and name it **new_feature**. In the first cell, add a simple Python statement: `print('hello world')`.
# MAGIC
# MAGIC 3. To the top of the notebook name within your Databricks Workspace, select the **dev** branch.
# MAGIC
# MAGIC 4. You should now see that one file has been changed. Add the commit message *update feature* and click **Commit & Push**.
# MAGIC
# MAGIC 5. The push should complete successfully to your GitHub repo. Close the GitHub popup in Databricks.
# MAGIC
# MAGIC 6. Go to GitHub and ensure you're on the **main** branch. You should notice that your new feature is not in the **main** branch yet.
# MAGIC
# MAGIC 7. In GitHub, switch to the **dev** branch (the default is **main**). Refresh the page if needed. The new notebook, **new_feature.ipynb**, should appear in your GitHub repo.
# MAGIC
# MAGIC 8. Leave GitHub open.

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Create a New Pull Request
# MAGIC
# MAGIC A pull request is a proposal to merge a set of changes from one branch into another. In a pull request, collaborators can review and discuss the proposed set of changes before they integrate the changes into the main codebase.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Complete the following steps to perform a pull request into the **main** branch:
# MAGIC
# MAGIC    a. In GitHub, select the **Pull requests** tab in the top navigation bar.
# MAGIC
# MAGIC    b. Click **New pull request**.
# MAGIC
# MAGIC    c. You will see two dropdown menus. Select the following in the **base** and **compare** drop downs.:
# MAGIC       - Change  first dropdown on the left to **main** (this is the base), 
# MAGIC       - Select **dev** (**compare**) on the right hand side as the branch to compare **main** to.
# MAGIC
# MAGIC    d. Scroll down and notice the updates in **dev** that are not in **main** (this includes the new notebook you created).
# MAGIC
# MAGIC    e. Click **Create pull request**.
# MAGIC
# MAGIC    f. In the **Open a pull request** section, you have several options. You can add a title and description for the pull request. On the right, you can specify **Reviewers**, **Assignees**, **Labels**, and more.
# MAGIC
# MAGIC    g. Fill out a title and description, then click **Create pull request**.
# MAGIC
# MAGIC    h. Leave the GitHub page open.

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Next, complete the following steps to merge the pull request. All of the commits from the **dev** branch will be added to the **main** branch.
# MAGIC
# MAGIC    a. On the next screen, after a few moments, you will see a message in the middle of your screen that says **This branch has no conflicts with the base branch** with a green check mark.
# MAGIC
# MAGIC    b. You can choose to add a comment to the pull request.
# MAGIC
# MAGIC    c. Merge the **dev** branch into **main** by selecting **Merge pull request**. Then, select **Confirm merge**. A note saying **Pull request successfully merged and closed** should appear. You can delete the **dev** branch if you choose to, but we will leave it.
# MAGIC
# MAGIC    d. In GitHub, navigate to **Code** in the top navigation bar and make sure you are in the **main** branch. Confirm that the **new_feature.ipynb** file is now in the **main** branch, and that the two branches are in sync.

# COMMAND ----------

# MAGIC %md
# MAGIC ## F. Pull within Databricks
# MAGIC As one final step, let's pull the GitHub changes to our local Workspace.
# MAGIC
# MAGIC 1. Go to **Workspace** and navigate to the location of your Git folder.
# MAGIC
# MAGIC 2. Select the **main** branch in your Git folder to open the Git folder popup.
# MAGIC
# MAGIC 3. Click **Pull** at the top-right of the popup to pull the most recent changes into your Workspace.
# MAGIC
# MAGIC 4. Close the Git folder popup and notice that the new file is now in the **main** branch of your Workspace.

# COMMAND ----------

# MAGIC %md
# MAGIC ## G. Delete your PAT
# MAGIC At the end of this lab you should delete your PAT token you created in this lab from your GitHub account. 
# MAGIC
# MAGIC However, when this lab ends this Vocareum lab account will cleared.

# COMMAND ----------

# MAGIC %md
# MAGIC # Next Steps (Advanced Deployment)
# MAGIC
# MAGIC The next step in your DevOps journey is to implement [GitHub Actions with DABs](https://docs.databricks.com/en/dev-tools/bundles/ci-cd-bundles.html). GitHub Actions can be used for continuous integration and delivery and are now in Public Preview. For a list of Databricks-specific GitHub actions like databricks/run-notebook, you can read the documentation [here](https://docs.databricks.com/aws/en/dev-tools/ci-cd/). The goal of GitHub Actions in general is to automate CI/CD workflows by automatically triggering pre-configured actions as a response to something happening in or to your GitHub repo - called an **Event**. For a full list of GitHub event types, see [this](https://docs.github.com/en/rest/using-the-rest-api/github-event-types?apiVersion=2022-11-28) documentation. 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
