# Data Engineering Zoomcamp - Module 2 (Kestra)

This folder contains the setup and flows to complete the Module 2 Homework/Quiz using Kestra.

## 1. Environment Setup

Run Kestra with PostgreSQL using Docker Compose:

1.  **Start Kestra**:
    ```bash
    docker compose up -d
    ```
2.  **Access UI**: Open `http://localhost:8080`.

*(Note: The `docker-compose.yml` is configured to mount the Docker socket, allowing Kestra to run local commands).*

---

## 2. The Solution Flow (`all_quiz_questions`)

This single Master Flow automates the entire quiz:
*   **Q1**: Checks file size for Yellow 2020-12.
*   **Q3**: Loops through entire year 2020 for Yellow Taxi.
*   **Q4**: Loops through entire year 2020 for Green Taxi.
*   **Q5**: Checks row count for Yellow 2021-03.
*   **Part 2 Challenge**: Loops through Jan-Jul 2021 for both.

### How to Run
1.  Copy the YAML code below.
2.  In Kestra, create a **New Flow**.
3.  Paste the code and click **Save**.
4.  Click **Execute**.

### Flow Code
```yaml
id: all_quiz_questions
namespace: zoomcamp

tasks:
  # Q3 (Yellow 2020) and Q1 (Yellow 2020-12)
  - id: yellow_2020_loop
    type: io.kestra.plugin.core.flow.ForEach
    values: ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
    tasks:
      - id: trigger_yellow_2020
        type: io.kestra.plugin.core.flow.Subflow
        flowId: taxi_etl_child
        namespace: zoomcamp
        wait: true
        inputs:
          taxi: "yellow"
          year: "2020"
          month: "{{ taskrun.value }}"

  # Q4 (Green 2020)
  - id: green_2020_loop
    type: io.kestra.plugin.core.flow.ForEach
    values: ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
    tasks:
      - id: trigger_green_2020
        type: io.kestra.plugin.core.flow.Subflow
        flowId: taxi_etl_child
        namespace: zoomcamp
        wait: true
        inputs:
          taxi: "green"
          year: "2020"
          month: "{{ taskrun.value }}"

  # Q5 (Yellow 2021-03)
  - id: yellow_2021_03
    type: io.kestra.plugin.core.flow.Subflow
    flowId: taxi_etl_child
    namespace: zoomcamp
    wait: true
    inputs:
      taxi: "yellow"
      year: "2021"
      month: "03"

  # Part 2 Challenge (Yellow/Green 2021 01-07)
  - id: challenge_2021_loop
    type: io.kestra.plugin.core.flow.ForEach
    values: ["yellow", "green"]
    tasks:
      - id: month_loop_challenge
        type: io.kestra.plugin.core.flow.ForEach
        values: ["01", "02", "03", "04", "05", "06", "07"]
        tasks:
          - id: trigger_challenge
            type: io.kestra.plugin.core.flow.Subflow
            flowId: taxi_etl_child
            namespace: zoomcamp
            wait: true
            inputs:
              taxi: "{{ parent.taskrun.value }}"
              year: "2021"
              month: "{{ taskrun.value }}"
```

---

## 3. The Child Flow (`taxi_etl_child`)

You also need this "Worker" flow saved in Kestra for the Master Flow to call.

```yaml
id: taxi_etl_child
namespace: zoomcamp

inputs:
  - id: taxi
    type: SELECT
    displayName: Taxi Type
    values: ["yellow", "green"]
    defaults: "yellow"
  - id: year
    type: SELECT
    displayName: Year
    values: ["2019", "2020", "2021"]
    defaults: "2020"
  - id: month
    type: SELECT
    displayName: Month
    values: ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
    defaults: "12"

variables:
  # Q2 Answer: Variable name evaluation
  file: "{{ inputs.taxi }}_tripdata_{{ inputs.year }}-{{ inputs.month }}.csv"

tasks:
  - id: download_csv
    type: io.kestra.plugin.core.http.Download
    uri: "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{{ inputs.taxi }}/{{ render(vars.file) }}.gz"

  - id: extract_data
    type: io.kestra.plugin.core.flow.WorkingDirectory
    tasks:
      - id: gunzip
        type: io.kestra.plugin.scripts.shell.Commands
        taskRunner:
          type: io.kestra.plugin.core.runner.Process
        commands:
          - cp "{{ outputs.download_csv.uri }}" "{{ render(vars.file) }}.gz"
          - gunzip "{{ render(vars.file) }}.gz"
      
      - id: get_stats
        type: io.kestra.plugin.scripts.shell.Commands
        taskRunner:
          type: io.kestra.plugin.core.runner.Process
        commands:
          - echo "--- [QUIZ Q1] FILE SIZE ---"
          - ls -lh
          - echo "--- [QUIZ Q3,4,5] ROW COUNT ---"
          - expr $(wc -l < "{{ render(vars.file) }}") - 1

triggers:
  - id: schedule
    type: io.kestra.plugin.core.trigger.Schedule
    cron: "0 9 1 * *"
    timezone: America/New_York # Q6 Answer
```

---

## 4. Quiz Answers

After running `all_quiz_questions`, check the logs or use these known values:

1.  **Q1**: **128.3 MiB** (`yellow_tripdata_2020-12.csv` size)
2.  **Q2**: **`green_tripdata_2020-04.csv`** (Variable evaluation)
3.  **Q3**: **24,648,499** (Yellow 2020 Total Rows)
4.  **Q4**: **1,734,051** (Green 2020 Total Rows)
5.  **Q5**: **1,925,152** (Yellow 2021-03 Rows)
6.  **Q6**: **`America/New_York`** (Timezone config)
