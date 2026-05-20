import psycopg2
import subprocess
import requests
import json
import re

# -----------------------------------
# CONNECT POSTGRES
# -----------------------------------

conn = psycopg2.connect(
    host="localhost",
    database="airflow_db",
    user="postgres",
    password="Dtsk2468@",
    port="5432"
)

cursor = conn.cursor()

print("\n================ CONNECTED TO POSTGRES ================\n")

# -----------------------------------
# FETCH UNRESOLVED RECORD
# -----------------------------------

cursor.execute("""

SELECT *
FROM workflow_validation
WHERE status = 'NOT_RESOLVED'
LIMIT 1

""")

record = cursor.fetchone()

print("\n================ UNRESOLVED RECORD ================\n")
print(record)

if not record:
    print("No unresolved records found")
    exit()

validation_id = record[0]
dag_name = record[1]

# -----------------------------------
# FETCH RELEVANT BUSINESS RULES
# -----------------------------------

cursor.execute("""

SELECT *
FROM business_rules
WHERE entity_name = 'employee'

""")

business_rules = cursor.fetchall()

print("\n================ BUSINESS RULES ================\n")
print(business_rules)

# -----------------------------------
# FETCH RELEVANT DAG METADATA
# -----------------------------------

cursor.execute("""

SELECT *
FROM dag_metadata
WHERE dag_name = 'employee_etl'

""")

dag_metadata = cursor.fetchall()

print("\n================ DAG METADATA ================\n")
print(dag_metadata)

# -----------------------------------
# FETCH RELEVANT DAG CODE
# -----------------------------------

print("\n================ FETCHING DAG CODE ================\n")

dag_file = f"/opt/airflow/dags/{dag_name}.py"

content_command = [
    "docker",
    "exec",
    "airflow",
    "cat",
    dag_file
]

dag_code = subprocess.check_output(
    content_command
).decode()

print("\n================ DAG CODE ================\n")
print(dag_code)

# -----------------------------------
# FETCH RELEVANT AIRFLOW LOGS
# -----------------------------------

print("\n================ FETCHING AIRFLOW LOGS ================\n")

log_command = [
    "docker",
    "exec",
    "airflow",
    "find",
    f"/opt/airflow/logs/dag_id={dag_name}",
    "-name",
    "*.log"
]

log_files = subprocess.check_output(
    log_command
).decode().splitlines()

latest_log = log_files[-1]

print("\nLATEST LOG FILE:\n")
print(latest_log)

log_content_command = [
    "docker",
    "exec",
    "airflow",
    "cat",
    latest_log
]

airflow_logs = subprocess.check_output(
    log_content_command
).decode()

# LIMIT LOG SIZE
airflow_logs = airflow_logs[:1500]

print("\n================ AIRFLOW LOGS ================\n")
print(airflow_logs)

# -----------------------------------
# BUILD CONTEXT
# -----------------------------------

context = f"""

SUSPICION:
{record}

BUSINESS RULES:
{business_rules}

DAG METADATA:
{dag_metadata}

RELEVANT DAG CODE:
{dag_code}

RELEVANT AIRFLOW LOGS:
{airflow_logs}

"""

print("\n================ FINAL CONTEXT ================\n")
print(context)

# -----------------------------------
# CREATE PROMPT
# -----------------------------------

prompt = f"""

You are a senior DataOps engineer.

A workflow anomaly was detected.

Investigate carefully.

Determine whether issue originated from:

1. Source data issue
2. DAG transformation logic issue
3. Technical failure

IMPORTANT:

Business rules are authoritative.

Do NOT classify as DATA_QUALITY
unless source data itself is invalid.

If source data is valid but DAG transformation
logic corrupts data, classify as CODE_ISSUE.

You MUST return ONLY valid JSON.

DO NOT explain.
DO NOT add notes.
DO NOT add markdown.
DO NOT add text before JSON.
DO NOT add text after JSON.

VALID RESPONSE FORMAT:

{{
  "classification": "CODE_ISSUE",
  "reason": "reason here",
  "suggested_fix": "fix here"
}}

CONTEXT:

{context}

"""

print("\n================ FINAL PROMPT ================\n")
print(prompt)

# -----------------------------------
# CALL OLLAMA
# -----------------------------------

print("\n================ CALLING OLLAMA ================\n")

response = requests.post(

    "http://localhost:11434/api/generate",

    json={
        "model": "llama3",
        "prompt": prompt,
        "stream": False,
        "temperature": 0
    }

)

result = response.json()

response_text = result["response"]

print("\n================ LLM RESPONSE ================\n")
print(response_text)

# -----------------------------------
# EXTRACT JSON
# -----------------------------------

print("\n================ EXTRACTING JSON ================\n")

parsed = None

try:

    # DIRECT JSON PARSE
    parsed = json.loads(response_text)

except:

    # FALLBACK REGEX EXTRACTION
    json_match = re.search(
        r'\{.*\}',
        response_text,
        re.DOTALL
    )

    if json_match:

        json_text = json_match.group()

        try:

            parsed = json.loads(json_text)

        except Exception as e:

            print("\nJSON EXTRACTION FAILED\n")
            print(e)

# -----------------------------------
# PROCESS PARSED JSON
# -----------------------------------

if parsed:

    print("\n================ PARSED JSON ================\n")

    print(parsed)

    print("\nCLASSIFICATION:")
    print(parsed["classification"])

    print("\nREASON:")
    print(parsed["reason"])

    print("\nSUGGESTED FIX:")
    print(parsed["suggested_fix"])

    # -----------------------------------
    # UPDATE VALIDATION TABLE
    # -----------------------------------

    print("\n================ UPDATING POSTGRES ================\n")

    update_query = """

    UPDATE workflow_validation

    SET

        llm_classification = %s,
        llm_reason = %s,
        suggested_fix = %s

    WHERE validation_id = %s

    """

    cursor.execute(

        update_query,

        (
            parsed["classification"],
            parsed["reason"],
            parsed["suggested_fix"],
            validation_id
        )
    )

    conn.commit()

    print("\nWORKFLOW VALIDATION UPDATED SUCCESSFULLY\n")

else:

    print("\nFAILED TO PARSE JSON\n")

# -----------------------------------
# VERIFY UPDATED RECORD
# -----------------------------------

cursor.execute("""

SELECT *
FROM workflow_validation
WHERE validation_id = %s

""", (validation_id,))

updated_record = cursor.fetchone()

print("\n================ UPDATED RECORD ================\n")
print(updated_record)

# -----------------------------------
# CLOSE CONNECTION
# -----------------------------------

cursor.close()
conn.close()

print("\n================ PROCESS COMPLETED ================\n")