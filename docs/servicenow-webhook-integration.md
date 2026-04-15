# Databricks Job Webhooks → ServiceNow Incident Integration

[Reference](https://community.databricks.com/t5/get-started-discussions/databricks-job-failure-service-now-integration/td-p/38337)

Automatically create ServiceNow incidents when Databricks jobs fail using native webhook notification destinations.

## Architecture

```
┌──────────────────┐        HTTPS POST         ┌──────────────────────┐
│  Databricks Job  │  ──── on_failure ────────► │  ServiceNow Scripted │
│  (fails)         │   webhook JSON payload     │  REST API            │
└──────────────────┘                            └──────────┬───────────┘
                                                           │
                                                           ▼
                                                ┌──────────────────────┐
                                                │  New Incident Record │
                                                │  (auto-created)      │
                                                └──────────────────────┘
```

**Flow:**
1. A Databricks job run fails.
2. Databricks fires an HTTPS POST to the configured webhook notification destination.
3. ServiceNow's Scripted REST API receives the payload and creates an incident with mapped fields.

---

## Prerequisites

| Requirement | Details |
|---|---|
| **ServiceNow instance** | Developer or production instance with Inbound REST API enabled |
| **ServiceNow role** | `web_service_admin` or `admin` to create Scripted REST APIs |
| **HTTPS endpoint** | ServiceNow must be reachable over HTTPS from Databricks control plane |
| **Databricks workspace** | Workspace admin access (to create notification destinations) |
| **Databricks CLI** | v0.200+ (optional, for bulk job configuration) |

---

## Step 1: ServiceNow Setup — Scripted REST API

Create a Scripted REST API resource in ServiceNow that accepts the Databricks webhook payload and creates an incident.

### 1.1 Create the Scripted REST API

1. Navigate to **System Web Services → Scripted REST APIs**.
2. Click **New**.
3. Set:
   - **Name:** `Databricks Job Failure`
   - **API ID:** `databricks_job_failure`
   - **Protection Policy:** leave as `None` (or set as needed)
4. Click **Submit**.

### 1.2 Create a Resource (POST endpoint)

1. Open the Scripted REST API you just created.
2. Under **Resources**, click **New**.
3. Set:
   - **Name:** `Create Incident`
   - **HTTP Method:** `POST`
   - **Relative path:** `/incident`
   - **Authentication:** checked (requires Basic Auth or OAuth token)
4. Paste the following **Server Script**:

```javascript
(function process(/*RESTAPIRequest*/ request, /*RESTAPIResponse*/ response) {

    var body = request.body.data;

    // Extract fields from Databricks webhook payload
    var jobId     = body.job_id    || 'unknown';
    var jobName   = body.job_name  || 'unknown';
    var runId     = body.run_id    || 'unknown';
    var runPageUrl = body.run_page_url || '';
    var workspaceUrl = body.workspace_url || '';
    var timestamp  = body.timestamp || new GlideDateTime().toString();

    // Build incident
    var inc = new GlideRecord('incident');
    inc.initialize();
    inc.short_description = 'Databricks Job Failure: ' + jobName + ' (Job ID: ' + jobId + ')';
    inc.description =
        'A Databricks job run has failed.\n\n' +
        'Job Name: ' + jobName + '\n' +
        'Job ID: ' + jobId + '\n' +
        'Run ID: ' + runId + '\n' +
        'Run URL: ' + runPageUrl + '\n' +
        'Workspace: ' + workspaceUrl + '\n' +
        'Timestamp: ' + timestamp;
    inc.urgency     = 2;        // Medium
    inc.impact      = 2;        // Medium
    inc.category    = 'Software';
    inc.subcategory = 'Job Failure';
    inc.assignment_group.setDisplayValue('Data Engineering');  // Adjust to your group
    inc.correlation_id = 'databricks-job-' + jobId + '-run-' + runId;  // Dedup key

    var sysId = inc.insert();

    response.setStatus(201);
    response.setBody({
        result: {
            incident_number: inc.number.toString(),
            sys_id: sysId,
            message: 'Incident created successfully'
        }
    });

})(request, response);
```

5. Click **Submit**.

### 1.3 Note Your Endpoint URL

Your endpoint will be:

```
https://<instance>.service-now.com/api/<scope>/databricks_job_failure/incident
```

For example: `https://dev12345.service-now.com/api/x_acme/databricks_job_failure/incident`

---

## Step 2: Create a Databricks Notification Destination

### Via the UI

1. Go to your Databricks workspace.
2. Navigate to **Admin Settings → Workspace Settings → Notification Destinations**.
3. Click **Add Destination**.
4. Set:
   - **Name:** `ServiceNow Incidents`
   - **Type:** `Webhook`
   - **URL:** Your ServiceNow endpoint URL from Step 1.3
5. Under **Custom Headers**, add an authorization header:
   - **Header:** `Authorization`
   - **Value:** `Basic <base64-encoded username:password>`

   To generate the Basic auth value:
   ```bash
   echo -n 'servicenow_user:password' | base64
   ```
6. Click **Save**.

### Via the Databricks CLI

```bash
databricks notification-destinations create \
  --display-name "ServiceNow Incidents" \
  --config '{
    "generic_webhook": {
      "url": "https://<instance>.service-now.com/api/<scope>/databricks_job_failure/incident",
      "username": "servicenow_user",
      "password": "your_password"
    }
  }'
```

Note the returned `id` — you'll need it for Step 3.

---

## Step 3: Configure Jobs to Send Failure Notifications

### Via the UI

1. Open the job in the **Workflows** UI.
2. Click **Edit** on the job.
3. Scroll to **Notifications** (or find it under the job-level settings, not task-level).
4. Click **Add Notification**.
5. Set:
   - **Event:** `On failure`
   - **Destination:** Select `ServiceNow Incidents`
6. **Save** the job.

### Via the Databricks CLI (single job)

```bash
# Get the current job config
databricks jobs get --job-id 123456 > job.json

# Add a webhook notification to the job settings
# The notification_destination_id comes from Step 2
databricks jobs update --job-id 123456 --json '{
  "new_settings": {
    "webhook_notifications": {
      "on_failure": [
        {
          "id": "<notification-destination-id>"
        }
      ]
    }
  }
}'
```

### Bulk-Configure Existing Jobs (CLI script)

```bash
#!/bin/bash
# bulk-add-webhook.sh
# Adds the ServiceNow webhook to all existing jobs' on_failure notifications.

DESTINATION_ID="<notification-destination-id>"

# List all job IDs
JOB_IDS=$(databricks jobs list --output json | jq -r '.[] | .job_id')

for JOB_ID in $JOB_IDS; do
  echo "Updating job $JOB_ID..."
  databricks jobs update --job-id "$JOB_ID" --json "{
    \"new_settings\": {
      \"webhook_notifications\": {
        \"on_failure\": [
          {
            \"id\": \"$DESTINATION_ID\"
          }
        ]
      }
    }
  }"
done

echo "Done. Updated $(echo "$JOB_IDS" | wc -w) jobs."
```

> **Warning:** This script overwrites existing `on_failure` webhook notifications. To preserve existing ones, fetch each job's current config first and merge the new destination into the existing array.

---

## Webhook Payload Reference

When a job triggers an `on_failure` event, Databricks sends an HTTPS POST with a JSON body like:

```json
{
  "event_type": "jobs.on_failure",
  "workspace_id": "1234567890",
  "workspace_url": "https://adb-1234567890.azuredatabricks.net",
  "job_id": "987654",
  "job_name": "nightly_etl_pipeline",
  "run_id": "11223344",
  "run_page_url": "https://adb-1234567890.azuredatabricks.net/#job/987654/run/11223344",
  "run_name": "nightly_etl_pipeline-2026-04-13",
  "run_type": "JOB_RUN",
  "run_start_time": "2026-04-13T02:00:00.000Z",
  "timestamp": "2026-04-13T02:15:32.000Z",
  "num_failures": 1,
  "errors": [
    {
      "error_code": "INTERNAL_ERROR",
      "message": "Run failed with exception: java.lang.OutOfMemoryError"
    }
  ]
}
```

> **Note:** The exact payload fields may vary. The fields above represent the commonly available fields. Some fields like `termination_code` or detailed cluster info are **not** included in webhook payloads — see [Limitations](#limitations--alternatives).

---

## ServiceNow Incident Field Mapping

| Databricks Webhook Field | ServiceNow Incident Field | Notes |
|---|---|---|
| `job_name` + `job_id` | `short_description` | e.g., "Databricks Job Failure: nightly_etl (ID: 987654)" |
| Full payload summary | `description` | Include run URL, workspace, timestamp, error messages |
| — | `urgency` | Set based on job priority; default to `2` (Medium) |
| — | `impact` | Set based on job criticality; default to `2` (Medium) |
| — | `category` | `Software` or a custom category |
| `job_id` + `run_id` | `correlation_id` | `databricks-job-{job_id}-run-{run_id}` for dedup |
| — | `assignment_group` | Route to the team that owns the job |
| `run_page_url` | Include in `description` | Direct link to the failed run for quick triage |

### Tips for Enriching Incidents

- **Dynamic urgency:** Check `job_name` against a list of critical pipelines in the ServiceNow script and set urgency to `1` (High) for production-critical jobs.
- **Assignment routing:** Use a lookup table or naming convention (e.g., job name prefix) to route to the correct assignment group.
- **Dedup with correlation_id:** ServiceNow can be configured to detect duplicate incidents using `correlation_id`. If a job retries and fails again, you can either create a new incident or update the existing one.

---

## Limitations & Alternatives

### Limitations of the Webhook Approach

| Limitation | Detail |
|---|---|
| **Max 3 destinations per event** | Each job supports up to 3 webhook notification destinations per event type (`on_failure`, `on_start`, `on_success`). |
| **Limited payload** | The webhook payload does not include `termination_code`, detailed cluster logs, task-level errors, or run duration. You must call the Databricks API separately for richer data. |
| **Per-job configuration** | Webhook notifications must be configured on each job individually — there is no workspace-level "all jobs" webhook setting. Use the bulk script above to mitigate. |
| **No payload customization** | You cannot modify the webhook JSON payload that Databricks sends. All transformation must happen on the ServiceNow side. |
| **No retry logic** | If ServiceNow is temporarily unavailable, the webhook delivery is not retried. There is no built-in dead-letter queue. |

### Alternatives for Advanced Needs

| Approach | When to Use |
|---|---|
| **SQL Alert on `system.lakeflow.job_run_timeline`** | Query the system table on a schedule and trigger a notification when new failures appear. Gives access to all run metadata including duration, termination codes, and cluster info. Good for batched alerting. |
| **Custom Script (notebook task)** | Add an `on_failure` dependent task to your job that runs a Python notebook. The notebook calls the ServiceNow REST API directly with a fully customized payload, including data from the Databricks Jobs API. Maximum flexibility. |
| **Databricks + Event Bridge / Pub/Sub** | Route job events through a cloud event bus for fan-out to multiple consumers, retry logic, and dead-letter handling. Best for enterprise-scale event-driven architectures. |

For this project's monitoring dashboard, the SQL Alert approach pairs well — you can reuse the same `system.lakeflow.job_run_timeline` queries already powering the dashboard to also trigger ServiceNow incidents.
