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
# FETCH UNRESOLVED CODE ISSUE
# -----------------------------------

cursor.execute("""

SELECT *
FROM workflow_validation

WHERE

    llm_classification = 'CODE_ISSUE'

AND

    status = 'NOT_RESOLVED'

LIMIT 1

""")

record = cursor.fetchone()

print("\n================ UNRESOLVED CODE ISSUE ================\n")
print(record)

if not record:

    print("\nNO CODE ISSUES FOUND\n")
    exit()

validation_id = record[0]
dag_name = record[1]

# -----------------------------------
# FETCH DAG FILE
# -----------------------------------

dag_file = f"/opt/airflow/dags/{dag_name}.py"

print("\n================ FETCHING DAG FILE ================\n")
print(dag_file)

read_command = [
    "docker",
    "exec",
    "airflow",
    "cat",
    dag_file
]

dag_code = subprocess.check_output(
    read_command
).decode()

print("\n================ ORIGINAL DAG CODE ================\n")
print(dag_code)

# -----------------------------------
# CALL OLLAMA
# -----------------------------------

print("\n================ CALLING OLLAMA ================\n")

prompt = f"""

You are a senior DataOps engineer.

A business logic issue exists in this Airflow DAG.

Your task:

1. Identify ONLY the incorrect transformation line
2. Generate ONLY corrected replacement line
3. Do NOT modify unrelated code
4. Do NOT return full DAG
5. Return STRICT JSON ONLY
6. Do NOT explain

FORMAT:

{{
    "old_code": "",
    "new_code": ""
}}

IMPORTANT:

The salary values in source data are already correct.

The transformation logic corrupts salary values.

Remove corruption logic completely.

CURRENT DAG CODE:

{dag_code}

"""

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

print("\n================ RAW LLM RESPONSE ================\n")
print(response_text)

# -----------------------------------
# EXTRACT JSON
# -----------------------------------

print("\n================ EXTRACTING JSON ================\n")

json_match = re.search(
    r'\{.*\}',
    response_text,
    re.DOTALL
)

if not json_match:

    print("\nFAILED TO EXTRACT JSON\n")
    exit()

parsed = json.loads(json_match.group())

old_code = parsed["old_code"]
new_code = parsed["new_code"]

print("\n================ OLD CODE ================\n")
print(old_code)

print("\n================ NEW CODE ================\n")
print(new_code)

# -----------------------------------
# APPLY SURGICAL PATCH
# -----------------------------------

patched_dag_code = dag_code.replace(
    old_code,
    new_code
)

print("\n================ PATCHED DAG ================\n")
print(patched_dag_code)

# -----------------------------------
# HUMAN APPROVAL STEP
# -----------------------------------

print("\n================ APPROVAL REQUIRED ================\n")

approval = input(
    "Type YES to apply patch or NO to reject: "
)

approval = approval.strip().upper()

print("\n================ APPROVAL STATUS ================\n")
print(approval)

# -----------------------------------
# HANDLE REJECTION
# -----------------------------------

if approval != "YES":

    print("\nPATCH REJECTED BY USER\n")

    reject_query = """

    UPDATE workflow_validation

    SET

        approval_status = 'REJECTED',

        resolution_notes = 'Patch rejected by user'

    WHERE validation_id = %s

    """

    cursor.execute(
        reject_query,
        (validation_id,)
    )

    conn.commit()

    cursor.close()
    conn.close()

    exit()

# -----------------------------------
# CREATE BACKUP
# -----------------------------------

print("\n================ CREATING BACKUP ================\n")

backup_file = f"{dag_name}_backup.py"

with open(backup_file, "w") as f:

    f.write(dag_code)

print("\nBACKUP CREATED\n")

# -----------------------------------
# WRITE PATCHED DAG LOCALLY
# -----------------------------------

print("\n================ WRITING PATCHED DAG ================\n")

patched_local_file = f"{dag_name}.py"

with open(patched_local_file, "w") as f:

    f.write(patched_dag_code)

print("\nPATCHED DAG WRITTEN LOCALLY\n")

# -----------------------------------
# COPY PATCHED DAG TO AIRFLOW
# -----------------------------------

print("\n================ COPYING DAG TO AIRFLOW ================\n")

copy_command = [

    "docker",
    "cp",
    patched_local_file,
    f"airflow:{dag_file}"

]

subprocess.run(copy_command)

print("\nPATCHED DAG COPIED TO AIRFLOW\n")

# -----------------------------------
# TRIGGER DAG AGAIN
# -----------------------------------

print("\n================ TRIGGERING DAG ================\n")

trigger_command = [

    "docker",
    "exec",
    "airflow",
    "airflow",
    "dags",
    "trigger",
    dag_name

]

subprocess.run(trigger_command)

print("\nDAG TRIGGERED SUCCESSFULLY\n")

# -----------------------------------
# UPDATE POSTGRES STATUS
# -----------------------------------

print("\n================ UPDATING POSTGRES ================\n")

update_query = """

UPDATE workflow_validation

SET

    status = 'RESOLVED',

    approval_status = 'APPROVED',

    resolution_notes = 'LLM generated surgical DAG patch successfully',

    resolved_at = NOW()

WHERE validation_id = %s

"""

cursor.execute(
    update_query,
    (validation_id,)
)

conn.commit()

print("\nWORKFLOW VALIDATION UPDATED\n")

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

print("\n================ SELF-HEALING COMPLETED ================\n")