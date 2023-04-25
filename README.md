# astra-dataflow-starter

This repository proposes some integration of Astra with Apache Beam and GCP Dataflow.

### Prerequisites

| Astra                        | GCP                                    | Local Environment          |
|-------------------------------------------|----------------------------------------|----------------------------|
| [Create Account](#1-get-an-astra-account) | [Create Project](#1-get-an-astra-account) | [Install Java](#1-get-an-astra-account) |
 | [Create Token](#2-get-an-astra-token)     | [Setup gCloud CLI](#2-get-an-astra-token) | [Install Maven](#1-get-an-astra-account) |
| [Setup CLI](#3-setup-astra-cli)           | [Setup Project](#2-get-an-astra-token) | [Clone and Build](#1-get-an-astra-account) |
| [Setup DB](#4-setup-databases)            |                                        |                            |

### Sample Pipelines

| Label                                         | Runner         | Description                                                        |
|-----------------------------------------------|----------------|--------------------------------------------------------------------|
| [Write Static Data ](#1-get-an-astra-account) | local (direct) | Load 100 record into an Astra Table                                |
| [Write Static Data ](#1-get-an-astra-account) | GCP (dataflow) | Load 100 record into an Astra Table. SC is in google cloud storage |


## Astra Prerequisites

### 1. Get an Astra Account

`✅` - Access [https://astra.datastax.com](https://astra.datastax.com) and register with `Google` or `Github` account

![](https://github.com/DataStax-Academy/cassandra-for-data-engineers/blob/main/images/setup-astra-1.png?raw=true)

### 2. Get an astra token

`✅` - Locate `Settings` (#1) in the menu on the left, then `Token Management` (#2)

`✅` - Select the role `Organization Administrator` before clicking `[Generate Token]`

![](https://github.com/DataStax-Academy/cassandra-for-data-engineers/blob/main/images/setup-astra-2.png?raw=true)

`✅` - Copy your token in the clipboard. With this token we will now create what is needed for the training.

![](https://github.com/DataStax-Academy/cassandra-for-data-engineers/blob/main/images/setup-astra-3.png?raw=true)

`✅` - Save you token as environment variable

```
export ASTRA_TOKEN=<paste_your_token_value_here>
```

### 3. Setup Astra CLI

`✅` - Install Cli
```
curl -Ls "https://dtsx.io/get-astra-cli" | bash
source ~/.astra/cli/astra-init.sh
```

`✅` - Setup CLI

```
astra setup --token ${ASTRA_TOKEN}
```

### 4. Setup Databases

`✅` - Create database `demo` with keyspace `demo`
```
astra db create demo -k demo
```

`✅` - Create table `simpledata`

```
astra db cqlsh demo -k demo \
  -e "CREATE TABLE IF NOT EXISTS simpledata(id int PRIMARY KEY, data text);" \
  --connect-timeout 20 \
  --request-timeout 20
```

`✅` - Validate table `simpledata` exists
```
astra db cqlsh demo -k demo \
  -e "select * from simpledata" \
  --connect-timeout 20 \
  --request-timeout 20
```

## Google Cloud Platform Prerequisites

### 1. Create project

`✅` - In the Google Cloud console, on the project selector page, select or [create a Google Cloud project](https://cloud.google.com/resource-manager/docs/creating-managing-projects)

> Note: If you don't plan to keep the resources that you create in this procedure, create a project instead of selecting an existing project. After you finish these steps, you can delete the project, removing all resources associated with the project.
Create a new Project in Google Cloud Console or select an existing one.

![](img/gcp-create-project.png)

`✅` - Make sure that billing is enabled for your Cloud project. Learn how to [check if billing is enabled on a project](https://cloud.google.com/billing/docs/how-to/verify-billing-enabled)

`✅` - Save project ID. The project identifier is available in the column `ID`. We will need it so let's save it as an environment variable
```
export GCP_PROJECT_ID=integrations-379317
export GCP_USER=cedrick.lunven@datastax.com
export GCP_COMPUTE_ENGINE=747469159044-compute@developer.gserviceaccount.com
```

### 2. Setup gCloud CLI

`✅` - Install gCloud CLI
```
curl https://sdk.cloud.google.com | bash
```

`✅` - Associated CLI with project in GCP

```
gcloud init
```

`✅` - Describe the project
```
gcloud projects describe ${GCP_PROJECT_ID}
```

### 3. Setup your project

`✅` - Enable APIS
```
gcloud services enable dataflow compute_component logging storage_component storage_api bigquery pubsub datastore.googleapis.com cloudresourcemanager.googleapis.com
```

`✅` - Add Roles. To complete the steps, your user account must have the Dataflow Admin role and the Service Account User role. The Compute Engine default service account must have the Dataflow Worker role. To add the required roles in the Google Cloud console:
```
gcloud projects add-iam-policy-binding ${GCP_PROJECT_ID} --member="user:${GCP_USER}" --role=roles/iam.serviceAccountUser
gcloud projects add-iam-policy-binding ${GCP_PROJECT_ID}  --member="serviceAccount:${GCP_COMPUTE_ENGINE}" --role=roles/dataflow.admin
gcloud projects add-iam-policy-binding ${GCP_PROJECT_ID}  --member="serviceAccount:${GCP_COMPUTE_ENGINE}" --role=roles/dataflow.worker
gcloud projects add-iam-policy-binding ${GCP_PROJECT_ID}  --member="serviceAccount:${GCP_COMPUTE_ENGINE}" --role=roles/storage.objectAdmin
```

## Local Environment Prerequisites

### 1. Tools

`✅` - Install [Java 11+](https://www.oracle.com/java/technologies/downloads/) 

`✅` - Install [Apache Maven](https://maven.apache.org/install.html)

### 2. Project 

`✅` - Clone and build project

```
git clone https://github.com/clun/astra-dataflow-starter.git
mvn clean install -Dmaven.test.skip=true
cd https://github.com/clun/astra-dataflow-starter/tree/main/samples-astra-beam-pipelines
```

`✅` - Download secure bundle

```
astra db download-scb demo -f /tmp/secure-connect-bundle-demo.zip
ls -l /tmp/secure-connect-bundle-demo.zip
```



----

### Example 1 - Load Simple Static Data (locally)

`✅` - Run Flow
```
 mvn compile exec:java -Pdirect-runner \
  -Dexec.mainClass=com.dtx.astra.pipelines.beam.BulkDataLoadWithBeam \
  -Dexec.args="\
    --keyspace=demo \
    --secureConnectBundle=/tmp/secure-connect-bundle-demo.zip \
    --token=${ASTRA_TOKEN}"
```

`✅` - Validate table `simpledata` has been populated
```
astra db cqlsh demo -k demo \
  -e "select * from simpledata" \
  --connect-timeout 20 \
  --request-timeout 20
```

### Example 2 - Load Simple Static Google Data Flow ()

`✅` - Create a `bucket` in the project
```
gsutil mb -c STANDARD -l US gs://astra_dataflow_inputs
gsutil mb -c STANDARD -l US gs://astra_dataflow_outputs
gsutil ls
```

`✅` - Copy Cloud Secure Bundle to GCS
```
gsutil cp /tmp/secure-connect-bundle-demo.zip gs://astra_dataflow_inputs/secure-connect-bundle-demo.zip
gsutil ls gs://astra_dataflow_inputs/
gsutil stat gs://astra_dataflow_inputs/secure-connect-bundle-demo.zip
```

`✅` - Make the secure connect bundle public

```
gsutil acl ch -u AllUsers:R gs://astra_dataflow_inputs/secure-connect-bundle-demo.zip
```

`✅` - Run the JOB
```
mvn -Pdataflow-runner compile exec:java \
    -Dexec.mainClass=com.dtx.astra.pipelines.beam.BulkDataLoadWithBeam \
    -Dexec.args="\
    --keyspace=demo \
    --secureConnectBundle=https://storage.googleapis.com/astra_dataflow_inputs/secure-connect-bundle-demo.zip \
    --token=${ASTRA_TOKEN} \
    --runner=DataflowRunner \
    --project=${GCP_PROJECT_ID} \
    --region=us-central1 \
    --gcpTempLocation=gs://dataflow-apache-quickstart_integrations-379317/temp/"  
```

`✅` - Show the populated table
```
astra db cqlsh demo -k demo \
  -e "select * from simpledata" \
  --connect-timeout 20 \
  --request-timeout 20
```

### Example 3 - 

```
astra db cqlsh demo -k demo \
  -e "truncate simpledata" \
  --connect-timeout 20 \
  --request-timeout 20
```


`✅` - Run Flow
```
 mvn compile exec:java -Pdirect-runner \
  -Dexec.mainClass=com.dtx.astra.pipelines.LoadStaticDataIntoAstraCql \
  -Dexec.args="\
    --keyspace=demo \
    --secureConnectBundle=/tmp/secure-connect-bundle-demo.zip \
    --token=${ASTRA_TOKEN}"
```


- Security
```
gcloud secrets add-iam-policy-binding cedrick-demo-scb \
        --member="serviceAccount:747469159044-compute@developer.gserviceaccount.com" \
        --role='roles/secretmanager.secretAccessor'
gcloud secrets add-iam-policy-binding cedrick-demo-scb \
        --member="serviceAccount:747469159044-compute@developer.gserviceaccount.com" \
        --role='roles/secretmanager.secretAccessor'
```

- Run
```
mvn -Pdataflow-runner compile exec:java \
    -Dexec.mainClass=ReadSecretAndConnectDataFlow \
    -Dexec.args="\
    --astraToken=projects/747469159044/secrets/astra-token/versions/1 \
    --secureConnectBundle=projects/747469159044/secrets/cedrick-demo-scb/versions/1 \
    --runner=DataflowRunner \
    --project=integrations-379317 \
    --region=us-central1 \
    --gcpTempLocation=gs://dataflow-apache-quickstart_integrations-379317/temp/"  
```
