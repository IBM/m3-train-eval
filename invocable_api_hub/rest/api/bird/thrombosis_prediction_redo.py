

from fastapi import APIRouter, Query
import sqlite3

app = APIRouter()

# Database connection
conn = sqlite3.connect('invocable_api_hub/db/bird/test/thrombosis_prediction.sqlite', check_same_thread=False)
cursor = conn.cursor()

# Endpoint to get the percentage of male admissions
@app.get("/v1/bird/thrombosis_prediction/male_admission_percentage", summary="Get the percentage of male admissions")
async def get_male_admission_percentage(sex: str = Query(..., description="Sex of the patient")):
    query = f"""
    SELECT CAST(SUM(CASE WHEN Admission = '+' THEN 1 ELSE 0 END) AS REAL) * 100 / SUM(CASE WHEN Admission = '-' THEN 1 ELSE 0 END)
    FROM Patient
    WHERE SEX = ?
    """
    cursor.execute(query, (sex,))
    result = cursor.fetchone()
    return {"percentage": result[0]}

# Endpoint to get the percentage of female patients born after a certain year
@app.get("/v1/bird/thrombosis_prediction/female_birth_percentage", summary="Get the percentage of female patients born after a certain year")
async def get_female_birth_percentage(year: int = Query(..., description="Year of birth")):
    query = f"""
    SELECT CAST(SUM(CASE WHEN STRFTIME('%Y', Birthday) > ? THEN 1 ELSE 0 END) AS REAL) * 100 / COUNT(*)
    FROM Patient
    WHERE SEX = 'F'
    """
    cursor.execute(query, (year,))
    result = cursor.fetchone()
    return {"percentage": result[0]}

# Endpoint to get the percentage of admissions between certain years
@app.get("/v1/bird/thrombosis_prediction/admission_percentage_between_years", summary="Get the percentage of admissions between certain years")
async def get_admission_percentage_between_years(start_year: int = Query(..., description="Start year"), end_year: int = Query(..., description="End year")):
    query = f"""
    SELECT CAST(SUM(CASE WHEN Admission = '+' THEN 1 ELSE 0 END) AS REAL) * 100 / COUNT(*)
    FROM Patient
    WHERE STRFTIME('%Y', Birthday) BETWEEN ? AND ?
    """
    cursor.execute(query, (start_year, end_year))
    result = cursor.fetchone()
    return {"percentage": result[0]}

# Endpoint to get the ratio of admissions for a specific diagnosis
@app.get("/v1/bird/thrombosis_prediction/admission_ratio_by_diagnosis", summary="Get the ratio of admissions for a specific diagnosis")
async def get_admission_ratio_by_diagnosis(diagnosis: str = Query(..., description="Diagnosis of the patient")):
    query = f"""
    SELECT SUM(CASE WHEN Admission = '+' THEN 1.0 ELSE 0 END) / SUM(CASE WHEN Admission = '-' THEN 1 ELSE 0 END)
    FROM Patient
    WHERE Diagnosis = ?
    """
    cursor.execute(query, (diagnosis,))
    result = cursor.fetchone()
    return {"ratio": result[0]}

# Endpoint to get patient diagnosis and lab date by patient ID
@app.get("/v1/bird/thrombosis_prediction/patient_diagnosis_lab_date", summary="Get patient diagnosis and lab date by patient ID")
async def get_patient_diagnosis_lab_date(patient_id: int = Query(..., description="ID of the patient")):
    query = f"""
    SELECT T1.Diagnosis, T2.Date
    FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    WHERE T1.ID = ?
    """
    cursor.execute(query, (patient_id,))
    result = cursor.fetchone()
    return {"diagnosis": result[0], "lab_date": result[1]}

# Endpoint to get patient details by patient ID
@app.get("/v1/bird/thrombosis_prediction/patient_details", summary="Get patient details by patient ID")
async def get_patient_details(patient_id: int = Query(..., description="ID of the patient")):
    query = f"""
    SELECT T1.SEX, T1.Birthday, T2.`Examination Date`, T2.Symptoms
    FROM Patient AS T1
    INNER JOIN Examination AS T2 ON T1.ID = T2.ID
    WHERE T1.ID = ?
    """
    cursor.execute(query, (patient_id,))
    result = cursor.fetchone()
    return {"sex": result[0], "birthday": result[1], "examination_date": result[2], "symptoms": result[3]}

# Endpoint to get distinct patient IDs with high LDH levels
@app.get("/v1/bird/thrombosis_prediction/high_ldh_patients", summary="Get distinct patient IDs with high LDH levels")
async def get_high_ldh_patients(ldh_threshold: float = Query(..., description="LDH threshold")):
    query = f"""
    SELECT DISTINCT T1.ID, T1.SEX, T1.Birthday
    FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    WHERE T2.LDH > ?
    """
    cursor.execute(query, (ldh_threshold,))
    result = cursor.fetchall()
    return [{"id": row[0], "sex": row[1], "birthday": row[2]} for row in result]

# Endpoint to get distinct patient IDs with positive RVVT
@app.get("/v1/bird/thrombosis_prediction/positive_rvvt_patients", summary="Get distinct patient IDs with positive RVVT")
async def get_positive_rvvt_patients():
    query = f"""
    SELECT DISTINCT T1.ID, STRFTIME('%Y', CURRENT_TIMESTAMP) - STRFTIME('%Y', T1.Birthday)
    FROM Patient AS T1
    INNER JOIN Examination AS T2 ON T1.ID = T2.ID
    WHERE T2.RVVT = '+'
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return [{"id": row[0], "age": row[1]} for row in result]

# Endpoint to get distinct patient IDs with specific thrombosis level
@app.get("/v1/bird/thrombosis_prediction/thrombosis_patients", summary="Get distinct patient IDs with specific thrombosis level")
async def get_thrombosis_patients(thrombosis_level: int = Query(..., description="Thrombosis level")):
    query = f"""
    SELECT DISTINCT T1.ID, T1.SEX, T1.Diagnosis
    FROM Patient AS T1
    INNER JOIN Examination AS T2 ON T1.ID = T2.ID
    WHERE T2.Thrombosis = ?
    """
    cursor.execute(query, (thrombosis_level,))
    result = cursor.fetchall()
    return [{"id": row[0], "sex": row[1], "diagnosis": row[2]} for row in result]

# Endpoint to get distinct patient IDs born in a specific year with high T-CHO
@app.get("/v1/bird/thrombosis_prediction/high_tcho_patients", summary="Get distinct patient IDs born in a specific year with high T-CHO")
async def get_high_tcho_patients(birth_year: int = Query(..., description="Year of birth"), tcho_threshold: float = Query(..., description="T-CHO threshold")):
    query = f"""
    SELECT DISTINCT T1.ID
    FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    WHERE STRFTIME('%Y', T1.Birthday) = ? AND T2.`T-CHO` >= ?
    """
    cursor.execute(query, (birth_year, tcho_threshold))
    result = cursor.fetchall()
    return [{"id": row[0]} for row in result]

# Endpoint to get distinct patient IDs with low ALB levels
@app.get("/v1/bird/thrombosis_prediction/low_alb_patients", summary="Get distinct patient IDs with low ALB levels")
async def get_low_alb_patients(alb_threshold: float = Query(..., description="ALB threshold")):
    query = f"""
    SELECT DISTINCT T1.ID, T1.SEX, T1.Diagnosis
    FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    WHERE T2.ALB < ?
    """
    cursor.execute(query, (alb_threshold,))
    result = cursor.fetchall()
    return [{"id": row[0], "sex": row[1], "diagnosis": row[2]} for row in result]

# Endpoint to get the percentage of female patients with abnormal TP levels
@app.get("/v1/bird/thrombosis_prediction/abnormal_tp_female_percentage", summary="Get the percentage of female patients with abnormal TP levels")
async def get_abnormal_tp_female_percentage():
    query = f"""
    SELECT CAST(SUM(CASE WHEN T1.SEX = 'F' AND (T2.TP < 6.0 OR T2.TP > 8.5) THEN 1 ELSE 0 END) AS REAL) * 100 / COUNT(*)
    FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    WHERE T1.SEX = 'F'
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"percentage": result[0]}

# Endpoint to get the average aCL IgG level for patients over a certain age with positive admission
@app.get("/v1/bird/thrombosis_prediction/average_acl_igg", summary="Get the average aCL IgG level for patients over a certain age with positive admission")
async def get_average_acl_igg(age_threshold: int = Query(..., description="Age threshold")):
    query = f"""
    SELECT AVG(T2.`aCL IgG`)
    FROM Patient AS T1
    INNER JOIN Examination AS T2 ON T1.ID = T2.ID
    WHERE STRFTIME('%Y', CURRENT_TIMESTAMP) - STRFTIME('%Y', T1.Birthday) >= ? AND T1.Admission = '+'
    """
    cursor.execute(query, (age_threshold,))
    result = cursor.fetchone()
    return {"average_acl_igg": result[0]}

# Endpoint to get the count of female patients with negative admission in a specific year
@app.get("/v1/bird/thrombosis_prediction/female_negative_admission_count", summary="Get the count of female patients with negative admission in a specific year")
async def get_female_negative_admission_count(year: int = Query(..., description="Year of description"), sex: str = Query(..., description="Sex of the patient"), admission: str = Query(..., description="Admission status")):
    query = f"""
    SELECT COUNT(*)
    FROM Patient
    WHERE STRFTIME('%Y', Description) = ? AND SEX = ? AND Admission = ?
    """
    cursor.execute(query, (year, sex, admission))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get the minimum age at first date
@app.get("/v1/bird/thrombosis_prediction/minimum_age_at_first_date", summary="Get the minimum age at first date")
async def get_minimum_age_at_first_date():
    query = f"""
    SELECT MIN(STRFTIME('%Y', `First Date`) - STRFTIME('%Y', Birthday))
    FROM Patient
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"minimum_age": result[0]}

# Endpoint to get the count of female patients with thrombosis in a specific year
@app.get("/v1/bird/thrombosis_prediction/female_thrombosis_count", summary="Get the count of female patients with thrombosis in a specific year")
async def get_female_thrombosis_count(year: int = Query(..., description="Year of examination"), thrombosis_level: int = Query(..., description="Thrombosis level")):
    query = f"""
    SELECT COUNT(*)
    FROM Patient AS T1
    INNER JOIN Examination AS T2 ON T1.ID = T2.ID
    WHERE T1.SEX = 'F' AND STRFTIME('%Y', T2.`Examination Date`) = ? AND T2.Thrombosis = ?
    """
    cursor.execute(query, (year, thrombosis_level))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get the age range of patients with high TG levels
@app.get("/v1/bird/thrombosis_prediction/age_range_high_tg", summary="Get the age range of patients with high TG levels")
async def get_age_range_high_tg(tg_threshold: float = Query(..., description="TG threshold")):
    query = f"""
    SELECT STRFTIME('%Y', MAX(T1.Birthday)) - STRFTIME('%Y', MIN(T1.Birthday))
    FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    WHERE T2.TG >= ?
    """
    cursor.execute(query, (tg_threshold,))
    result = cursor.fetchone()
    return {"age_range": result[0]}

# Endpoint to get the latest patient with symptoms
@app.get("/v1/bird/thrombosis_prediction/latest_patient_with_symptoms", summary="Get the latest patient with symptoms")
async def get_latest_patient_with_symptoms():
    query = f"""
    SELECT T2.Symptoms, T1.Diagnosis
    FROM Patient AS T1
    INNER JOIN Examination AS T2 ON T1.ID = T2.ID
    WHERE T2.Symptoms IS NOT NULL
    ORDER BY T1.Birthday DESC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"symptoms": result[0], "diagnosis": result[1]}

# Endpoint to get the monthly average patient count with high LDH levels
@app.get("/v1/bird/thrombosis_prediction/monthly_average_high_ldh", summary="Get the monthly average patient count with high LDH levels")
async def get_monthly_average_high_ldh(year: int = Query(..., description="Year of laboratory date"), sex: str = Query(..., description="Sex of the patient")):
    query = f"""
    SELECT CAST(COUNT(T1.ID) AS REAL) / 12
    FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    WHERE STRFTIME('%Y', T2.Date) = ? AND T1.SEX = ?
    """
    cursor.execute(query, (year, sex))
    result = cursor.fetchone()
    return {"monthly_average": result[0]}

# Endpoint to get the youngest patient with SJS diagnosis
@app.get("/v1/bird/thrombosis_prediction/youngest_patient_with_sjs", summary="Get the youngest patient with SJS diagnosis")
async def get_youngest_patient_with_sjs():
    query = f"""
    SELECT T1.Date, STRFTIME('%Y', T2.`First Date`) - STRFTIME('%Y', T2.Birthday), T2.Birthday
    FROM Laboratory AS T1
    INNER JOIN Patient AS T2 ON T1.ID = T2.ID
    WHERE T2.Diagnosis = 'SJS' AND T2.Birthday IS NOT NULL
    ORDER BY T2.Birthday ASC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"lab_date": result[0], "age": result[1], "birthday": result[2]}


# Endpoint to get the ratio of male patients with UA <= 8.0 to female patients with UA <= 6.5
@app.get("/v1/bird/thrombosis_prediction/patient_ratio", summary="Get the ratio of male patients with UA <= 8.0 to female patients with UA <= 6.5")
async def get_patient_ratio(ua_male: float = Query(..., description="UA value for male patients"), ua_female: float = Query(..., description="UA value for female patients")):
    query = f"""
    SELECT CAST(SUM(CASE WHEN T2.UA <= {ua_male} AND T1.SEX = 'M' THEN 1 ELSE 0 END) AS REAL) /
           SUM(CASE WHEN T2.UA <= {ua_female} AND T1.SEX = 'F' THEN 1 ELSE 0 END)
    FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"ratio": result[0]}

# Endpoint to get the count of distinct patient IDs with specific admission and examination date criteria
@app.get("/v1/bird/thrombosis_prediction/patient_count", summary="Get the count of distinct patient IDs with specific admission and examination date criteria")
async def get_patient_count(admission: str = Query(..., description="Admission status"), years_diff: int = Query(..., description="Years difference between examination and first date")):
    query = f"""
    SELECT COUNT(DISTINCT T1.ID)
    FROM Patient AS T1
    INNER JOIN Examination AS T2 ON T1.ID = T2.ID
    WHERE T1.Admission = '{admission}' AND STRFTIME('%Y', T2.`Examination Date`) - STRFTIME('%Y', T1.`First Date`) >= {years_diff}
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get the count of patients with specific examination date and age criteria
@app.get("/v1/bird/thrombosis_prediction/patient_age_count", summary="Get the count of patients with specific examination date and age criteria")
async def get_patient_age_count(start_year: int = Query(..., description="Start year of examination date"), end_year: int = Query(..., description="End year of examination date"), age_limit: int = Query(..., description="Age limit")):
    query = f"""
    SELECT COUNT(T1.ID)
    FROM Patient AS T1
    INNER JOIN Examination AS T2 ON T1.ID = T2.ID
    WHERE STRFTIME('%Y', T2.`Examination Date`) BETWEEN '{start_year}' AND '{end_year}'
    AND STRFTIME('%Y', T2.`Examination Date`) - STRFTIME('%Y', T1.Birthday) < {age_limit}
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get the count of distinct patient IDs with specific T-BIL and sex criteria
@app.get("/v1/bird/thrombosis_prediction/patient_tbil_count", summary="Get the count of distinct patient IDs with specific T-BIL and sex criteria")
async def get_patient_tbil_count(tbil: float = Query(..., description="T-BIL value"), sex: str = Query(..., description="Sex of the patient")):
    query = f"""
    SELECT COUNT(DISTINCT T1.ID)
    FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    WHERE T2.`T-BIL` >= {tbil} AND T1.SEX = '{sex}'
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get the most common diagnosis within a specific date range
@app.get("/v1/bird/thrombosis_prediction/common_diagnosis", summary="Get the most common diagnosis within a specific date range")
async def get_common_diagnosis(start_date: str = Query(..., description="Start date of examination"), end_date: str = Query(..., description="End date of examination")):
    query = f"""
    SELECT T2.Diagnosis
    FROM Examination AS T1
    INNER JOIN Patient AS T2 ON T1.ID = T2.ID
    WHERE T1.`Examination Date` BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY T2.Diagnosis
    ORDER BY COUNT(T2.Diagnosis) DESC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"diagnosis": result[0]}

# Endpoint to get the average age of patients within a specific date range
@app.get("/v1/bird/thrombosis_prediction/average_age", summary="Get the average age of patients within a specific date range")
async def get_average_age(start_date: str = Query(..., description="Start date of laboratory date"), end_date: str = Query(..., description="End date of laboratory date"), year: int = Query(..., description="Year to calculate age")):
    query = f"""
    SELECT AVG({year} - STRFTIME('%Y', T2.Birthday))
    FROM Laboratory AS T1
    INNER JOIN Patient AS T2 ON T1.ID = T2.ID
    WHERE T1.Date BETWEEN '{start_date}' AND '{end_date}'
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"average_age": result[0]}

# Endpoint to get the age and diagnosis of the patient with the highest HGB value
@app.get("/v1/bird/thrombosis_prediction/highest_hgb", summary="Get the age and diagnosis of the patient with the highest HGB value")
async def get_highest_hgb():
    query = """
    SELECT STRFTIME('%Y', T2.Date) - STRFTIME('%Y', T1.Birthday), T1.Diagnosis
    FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    ORDER BY T2.HGB DESC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"age": result[0], "diagnosis": result[1]}

# Endpoint to get the ANA value for a specific examination
@app.get("/v1/bird/thrombosis_prediction/ana_value", summary="Get the ANA value for a specific examination")
async def get_ana_value(id: int = Query(..., description="ID of the examination"), date: str = Query(..., description="Examination date")):
    query = f"""
    SELECT ANA
    FROM Examination
    WHERE ID = {id} AND `Examination Date` = '{date}'
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"ana": result[0]}

# Endpoint to get the T-CHO status for a specific laboratory record
@app.get("/v1/bird/thrombosis_prediction/tcho_status", summary="Get the T-CHO status for a specific laboratory record")
async def get_tcho_status(id: int = Query(..., description="ID of the laboratory record"), date: str = Query(..., description="Laboratory date")):
    query = f"""
    SELECT CASE WHEN `T-CHO` < 250 THEN 'Normal' ELSE 'Abnormal' END
    FROM Laboratory
    WHERE ID = {id} AND Date = '{date}'
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"status": result[0]}

# Endpoint to get the sex of the patient with a specific diagnosis and first date
@app.get("/v1/bird/thrombosis_prediction/patient_sex", summary="Get the sex of the patient with a specific diagnosis and first date")
async def get_patient_sex(diagnosis: str = Query(..., description="Diagnosis of the patient")):
    query = f"""
    SELECT SEX
    FROM Patient
    WHERE Diagnosis = '{diagnosis}' AND `First Date` IS NOT NULL
    ORDER BY `First Date` ASC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"sex": result[0]}

# Endpoint to get the aCL values for a specific examination
@app.get("/v1/bird/thrombosis_prediction/acl_values", summary="Get the aCL values for a specific examination")
async def get_acl_values(diagnosis: str = Query(..., description="Diagnosis of the patient"), description: str = Query(..., description="Description of the patient"), date: str = Query(..., description="Examination date")):
    query = f"""
    SELECT `aCL IgA`, `aCL IgG`, `aCL IgM`
    FROM Examination
    WHERE ID IN (
        SELECT ID
        FROM Patient
        WHERE Diagnosis = '{diagnosis}' AND Description = '{description}'
    ) AND `Examination Date` = '{date}'
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"aCL IgA": result[0], "aCL IgG": result[1], "aCL IgM": result[2]}

# Endpoint to get the sex of the patient with a specific GPT value and date
@app.get("/v1/bird/thrombosis_prediction/patient_sex_gpt", summary="Get the sex of the patient with a specific GPT value and date")
async def get_patient_sex_gpt(gpt: float = Query(..., description="GPT value"), date: str = Query(..., description="Laboratory date")):
    query = f"""
    SELECT T1.SEX
    FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    WHERE T2.GPT = {gpt} AND T2.Date = '{date}'
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"sex": result[0]}

# Endpoint to get the age of the patient with a specific UA value and date
@app.get("/v1/bird/thrombosis_prediction/patient_age_ua", summary="Get the age of the patient with a specific UA value and date")
async def get_patient_age_ua(ua: float = Query(..., description="UA value"), date: str = Query(..., description="Laboratory date")):
    query = f"""
    SELECT STRFTIME('%Y', T2.Date) - STRFTIME('%Y', T1.Birthday)
    FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    WHERE T2.UA = {ua} AND T2.Date = '{date}'
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"age": result[0]}

# Endpoint to get the count of laboratory records for a specific patient and year
@app.get("/v1/bird/thrombosis_prediction/lab_count", summary="Get the count of laboratory records for a specific patient and year")
async def get_lab_count(first_date: str = Query(..., description="First date of the patient"), diagnosis: str = Query(..., description="Diagnosis of the patient"), year: int = Query(..., description="Year of the laboratory date")):
    query = f"""
    SELECT COUNT(*)
    FROM Laboratory
    WHERE ID = (
        SELECT ID
        FROM Patient
        WHERE `First Date` = '{first_date}' AND Diagnosis = '{diagnosis}'
    ) AND STRFTIME('%Y', Date) = '{year}'
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get the diagnosis of the patient with a specific examination date
@app.get("/v1/bird/thrombosis_prediction/patient_diagnosis", summary="Get the diagnosis of the patient with a specific examination date")
async def get_patient_diagnosis(examination_date: str = Query(..., description="Examination date"), diagnosis: str = Query(..., description="Diagnosis of the examination")):
    query = f"""
    SELECT T1.Diagnosis
    FROM Patient AS T1
    INNER JOIN Examination AS T2 ON T1.ID = T2.ID
    WHERE T1.ID = (
        SELECT ID
        FROM Examination
        WHERE `Examination Date` = '{examination_date}' AND Diagnosis = '{diagnosis}'
    ) AND T2.`Examination Date` = T1.`First Date`
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"diagnosis": result[0]}

# Endpoint to get the symptoms of the patient with a specific birthday and examination date
@app.get("/v1/bird/thrombosis_prediction/patient_symptoms", summary="Get the symptoms of the patient with a specific birthday and examination date")
async def get_patient_symptoms(birthday: str = Query(..., description="Birthday of the patient"), examination_date: str = Query(..., description="Examination date")):
    query = f"""
    SELECT T2.Symptoms
    FROM Patient AS T1
    INNER JOIN Examination AS T2 ON T1.ID = T2.ID
    WHERE T1.Birthday = '{birthday}' AND T2.`Examination Date` = '{examination_date}'
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"symptoms": result[0]}

# Endpoint to get the T-CHO difference ratio for a specific patient and date range
@app.get("/v1/bird/thrombosis_prediction/tcho_difference_ratio", summary="Get the T-CHO difference ratio for a specific patient and date range")
async def get_tcho_difference_ratio(birthday: str = Query(..., description="Birthday of the patient"), month1: str = Query(..., description="First month of the date range"), month2: str = Query(..., description="Second month of the date range")):
    query = f"""
    SELECT CAST((SUM(CASE WHEN T2.Date LIKE '{month1}' THEN T2.`T-CHO` ELSE 0 END) - SUM(CASE WHEN T2.Date LIKE '{month2}' THEN T2.`T-CHO` ELSE 0 END)) AS REAL) /
           SUM(CASE WHEN T2.Date LIKE '{month2}' THEN T2.`T-CHO` ELSE 0 END)
    FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    WHERE T1.Birthday = '{birthday}'
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"ratio": result[0]}

# Endpoint to get the IDs of examinations with a specific diagnosis and date range
@app.get("/v1/bird/thrombosis_prediction/examination_ids", summary="Get the IDs of examinations with a specific diagnosis and date range")
async def get_examination_ids(start_date: str = Query(..., description="Start date of the examination"), end_date: str = Query(..., description="End date of the examination"), diagnosis: str = Query(..., description="Diagnosis of the examination")):
    query = f"""
    SELECT ID
    FROM Examination
    WHERE `Examination Date` BETWEEN '{start_date}' AND '{end_date}' AND Diagnosis = '{diagnosis}'
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return {"ids": [row[0] for row in result]}

# Endpoint to get the distinct IDs of laboratory records with specific criteria
@app.get("/v1/bird/thrombosis_prediction/distinct_lab_ids", summary="Get the distinct IDs of laboratory records with specific criteria")
async def get_distinct_lab_ids(start_date: str = Query(..., description="Start date of the laboratory date"), end_date: str = Query(..., description="End date of the laboratory date"), gpt: float = Query(..., description="GPT value"), alb: float = Query(..., description="ALB value")):
    query = f"""
    SELECT DISTINCT ID
    FROM Laboratory
    WHERE Date BETWEEN '{start_date}' AND '{end_date}' AND GPT > {gpt} AND ALB < {alb}
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return {"ids": [row[0] for row in result]}

# Endpoint to get the IDs of patients with specific criteria
@app.get("/v1/bird/thrombosis_prediction/patient_ids", summary="Get the IDs of patients with specific criteria")
async def get_patient_ids(birth_year: int = Query(..., description="Birth year of the patient"), sex: str = Query(..., description="Sex of the patient"), admission: str = Query(..., description="Admission status")):
    query = f"""
    SELECT ID
    FROM Patient
    WHERE STRFTIME('%Y', Birthday) = '{birth_year}' AND SEX = '{sex}' AND Admission = '{admission}'
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return {"ids": [row[0] for row in result]}

# Endpoint to get count of examinations with specific conditions
@app.get("/v1/bird/thrombosis_prediction/examination_count", summary="Get count of examinations with specific conditions")
async def get_examination_count(thrombosis: int = Query(..., description="Thrombosis value"),
                                ana_pattern: str = Query(..., description="ANA Pattern value"),
                                acl_igm_multiplier: float = Query(..., description="Multiplier for aCL IgM")):
    query = f"""
    SELECT COUNT(*) FROM Examination
    WHERE Thrombosis = ? AND `ANA Pattern` = ? AND `aCL IgM` > (
        SELECT AVG(`aCL IgM`) * ? FROM Examination
        WHERE Thrombosis = ? AND `ANA Pattern` = ?
    )
    """
    cursor.execute(query, (thrombosis, ana_pattern, acl_igm_multiplier, thrombosis, ana_pattern))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get percentage of laboratory records with specific conditions
@app.get("/v1/bird/thrombosis_prediction/laboratory_percentage", summary="Get percentage of laboratory records with specific conditions")
async def get_laboratory_percentage(ua_threshold: float = Query(..., description="UA threshold value"),
                                    u_pro_min: float = Query(..., description="Minimum U-PRO value"),
                                    u_pro_max: float = Query(..., description="Maximum U-PRO value")):
    query = f"""
    SELECT CAST(SUM(CASE WHEN UA <= ? THEN 1 ELSE 0 END) AS REAL) * 100 / COUNT(ID)
    FROM Laboratory
    WHERE `U-PRO` > ? AND `U-PRO` < ?
    """
    cursor.execute(query, (ua_threshold, u_pro_min, u_pro_max))
    result = cursor.fetchone()
    return {"percentage": result[0]}

# Endpoint to get percentage of patients with specific conditions
@app.get("/v1/bird/thrombosis_prediction/patient_percentage", summary="Get percentage of patients with specific conditions")
async def get_patient_percentage(diagnosis: str = Query(..., description="Diagnosis value"),
                                 year: str = Query(..., description="Year value"),
                                 sex: str = Query(..., description="Sex value")):
    query = f"""
    SELECT CAST(SUM(CASE WHEN Diagnosis = ? THEN 1 ELSE 0 END) AS REAL) * 100 / COUNT(ID)
    FROM Patient
    WHERE STRFTIME('%Y', `First Date`) = ? AND SEX = ?
    """
    cursor.execute(query, (diagnosis, year, sex))
    result = cursor.fetchone()
    return {"percentage": result[0]}

# Endpoint to get distinct patient IDs with specific conditions
@app.get("/v1/bird/thrombosis_prediction/distinct_patient_ids", summary="Get distinct patient IDs with specific conditions")
async def get_distinct_patient_ids(admission: str = Query(..., description="Admission value"),
                                   t_bil_max: float = Query(..., description="Maximum T-BIL value"),
                                   date_pattern: str = Query(..., description="Date pattern")):
    query = f"""
    SELECT DISTINCT T1.ID
    FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    WHERE T1.Admission = ? AND T2.`T-BIL` < ? AND T2.Date LIKE ?
    """
    cursor.execute(query, (admission, t_bil_max, date_pattern))
    result = cursor.fetchall()
    return {"patient_ids": [row[0] for row in result]}

# Endpoint to get count of distinct patient IDs with specific conditions
@app.get("/v1/bird/thrombosis_prediction/distinct_patient_count", summary="Get count of distinct patient IDs with specific conditions")
async def get_distinct_patient_count(ana_pattern: str = Query(..., description="ANA Pattern value"),
                                     birth_year_min: str = Query(..., description="Minimum birth year"),
                                     birth_year_max: str = Query(..., description="Maximum birth year"),
                                     sex: str = Query(..., description="Sex value")):
    query = f"""
    SELECT COUNT(DISTINCT T1.ID)
    FROM Patient AS T1
    INNER JOIN Examination AS T2 ON T1.ID = T2.ID
    WHERE T2.`ANA Pattern` != ? AND STRFTIME('%Y', T1.Birthday) BETWEEN ? AND ? AND T1.SEX = ?
    """
    cursor.execute(query, (ana_pattern, birth_year_min, birth_year_max, sex))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get patient sex with specific conditions
@app.get("/v1/bird/thrombosis_prediction/patient_sex", summary="Get patient sex with specific conditions")
async def get_patient_sex(diagnosis: str = Query(..., description="Diagnosis value"),
                          crp: str = Query(..., description="CRP value"),
                          cre: float = Query(..., description="CRE value"),
                          ldh: float = Query(..., description="LDH value")):
    query = f"""
    SELECT T1.SEX
    FROM Patient AS T1
    INNER JOIN Examination AS T2 ON T1.ID = T2.ID
    INNER JOIN Laboratory AS T3 ON T3.ID = T2.ID
    WHERE T2.Diagnosis = ? AND T3.CRP = ? AND T3.CRE = ? AND T3.LDH = ?
    """
    cursor.execute(query, (diagnosis, crp, cre, ldh))
    result = cursor.fetchall()
    return {"sex": [row[0] for row in result]}

# Endpoint to get average ALB with specific conditions
@app.get("/v1/bird/thrombosis_prediction/average_alb", summary="Get average ALB with specific conditions")
async def get_average_alb(plt_min: int = Query(..., description="Minimum PLT value"),
                          diagnosis: str = Query(..., description="Diagnosis value"),
                          sex: str = Query(..., description="Sex value")):
    query = f"""
    SELECT AVG(T2.ALB)
    FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    WHERE T2.PLT > ? AND T1.Diagnosis = ? AND T1.SEX = ?
    """
    cursor.execute(query, (plt_min, diagnosis, sex))
    result = cursor.fetchone()
    return {"average_alb": result[0]}

# Endpoint to get most common symptoms with specific conditions
@app.get("/v1/bird/thrombosis_prediction/most_common_symptoms", summary="Get most common symptoms with specific conditions")
async def get_most_common_symptoms(diagnosis: str = Query(..., description="Diagnosis value")):
    query = f"""
    SELECT Symptoms
    FROM Examination
    WHERE Diagnosis = ?
    GROUP BY Symptoms
    ORDER BY COUNT(Symptoms) DESC
    LIMIT 1
    """
    cursor.execute(query, (diagnosis,))
    result = cursor.fetchone()
    return {"most_common_symptom": result[0]}

# Endpoint to get first date and diagnosis for a specific patient
@app.get("/v1/bird/thrombosis_prediction/patient_first_date_diagnosis", summary="Get first date and diagnosis for a specific patient")
async def get_patient_first_date_diagnosis(patient_id: int = Query(..., description="Patient ID")):
    query = f"""
    SELECT `First Date`, Diagnosis
    FROM Patient
    WHERE ID = ?
    """
    cursor.execute(query, (patient_id,))
    result = cursor.fetchone()
    return {"first_date": result[0], "diagnosis": result[1]}

# Endpoint to get count of patients with specific conditions
@app.get("/v1/bird/thrombosis_prediction/patient_count", summary="Get count of patients with specific conditions")
async def get_patient_count(sex: str = Query(..., description="Sex value"),
                            diagnosis: str = Query(..., description="Diagnosis value")):
    query = f"""
    SELECT COUNT(ID)
    FROM Patient
    WHERE SEX = ? AND Diagnosis = ?
    """
    cursor.execute(query, (sex, diagnosis))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get count of laboratory records with specific conditions
@app.get("/v1/bird/thrombosis_prediction/laboratory_count", summary="Get count of laboratory records with specific conditions")
async def get_laboratory_count(alb_min: float = Query(..., description="Minimum ALB value"),
                               alb_max: float = Query(..., description="Maximum ALB value"),
                               year: str = Query(..., description="Year value")):
    query = f"""
    SELECT COUNT(ID)
    FROM Laboratory
    WHERE (ALB <= ? OR ALB >= ?) AND STRFTIME('%Y', Date) = ?
    """
    cursor.execute(query, (alb_min, alb_max, year))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get percentage of examinations with specific conditions
@app.get("/v1/bird/thrombosis_prediction/examination_percentage", summary="Get percentage of examinations with specific conditions")
async def get_examination_percentage(diagnosis: str = Query(..., description="Diagnosis value"),
                                     symptoms: str = Query(..., description="Symptoms value")):
    query = f"""
    SELECT CAST(SUM(CASE WHEN Diagnosis = ? THEN 1 ELSE 0 END) AS REAL) * 100 / COUNT(ID)
    FROM Examination
    WHERE Symptoms = ?
    """
    cursor.execute(query, (diagnosis, symptoms))
    result = cursor.fetchone()
    return {"percentage": result[0]}

# Endpoint to get percentage of patients with specific conditions
@app.get("/v1/bird/thrombosis_prediction/patient_percentage_by_year", summary="Get percentage of patients with specific conditions by year")
async def get_patient_percentage_by_year(sex: str = Query(..., description="Sex value"),
                                         diagnosis: str = Query(..., description="Diagnosis value"),
                                         year: str = Query(..., description="Year value")):
    query = f"""
    SELECT CAST(SUM(CASE WHEN SEX = ? THEN 1 ELSE 0 END) AS REAL) * 100 / COUNT(ID)
    FROM Patient
    WHERE Diagnosis = ? AND STRFTIME('%Y', Birthday) = ?
    """
    cursor.execute(query, (sex, diagnosis, year))
    result = cursor.fetchone()
    return {"percentage": result[0]}

# Endpoint to get count of patients with specific conditions
@app.get("/v1/bird/thrombosis_prediction/patient_count_by_diagnosis", summary="Get count of patients with specific conditions by diagnosis")
async def get_patient_count_by_diagnosis(diagnosis: str = Query(..., description="Diagnosis value"),
                                         sex: str = Query(..., description="Sex value"),
                                         year_min: str = Query(..., description="Minimum year"),
                                         year_max: str = Query(..., description="Maximum year"),
                                         admission: str = Query(..., description="Admission value")):
    query = f"""
    SELECT COUNT(T1.ID)
    FROM Patient AS T1
    INNER JOIN Examination AS T2 ON T1.ID = T2.ID
    WHERE T2.Diagnosis = ? AND T1.SEX = ? AND STRFTIME('%Y', T2.`Examination Date`) BETWEEN ? AND ? AND T1.Admission = ?
    """
    cursor.execute(query, (diagnosis, sex, year_min, year_max, admission))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get count of patients with specific conditions
@app.get("/v1/bird/thrombosis_prediction/patient_count_by_wbc", summary="Get count of patients with specific conditions by WBC")
async def get_patient_count_by_wbc(wbc_max: float = Query(..., description="Maximum WBC value"),
                                   sex: str = Query(..., description="Sex value")):
    query = f"""
    SELECT COUNT(T1.ID)
    FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    WHERE T2.WBC < ? AND T1.SEX = ?
    """
    cursor.execute(query, (wbc_max, sex))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get date difference for a specific patient
@app.get("/v1/bird/thrombosis_prediction/date_difference", summary="Get date difference for a specific patient")
async def get_date_difference(patient_id: int = Query(..., description="Patient ID")):
    query = f"""
    SELECT STRFTIME('%d', T3.`Examination Date`) - STRFTIME('%d', T1.`First Date`)
    FROM Patient AS T1
    INNER JOIN Examination AS T3 ON T1.ID = T3.ID
    WHERE T1.ID = ?
    """
    cursor.execute(query, (patient_id,))
    result = cursor.fetchone()
    return {"date_difference": result[0]}

# Endpoint to get boolean value based on conditions
@app.get("/v1/bird/thrombosis_prediction/boolean_value", summary="Get boolean value based on conditions")
async def get_boolean_value(patient_id: int = Query(..., description="Patient ID"),
                            sex: str = Query(..., description="Sex value"),
                            ua_threshold: float = Query(..., description="UA threshold value")):
    query = f"""
    SELECT CASE
        WHEN (T1.SEX = ? AND T2.UA > ?) OR (T1.SEX = 'M' AND T2.UA > 8.0) THEN true
        ELSE false
    END
    FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    WHERE T1.ID = ?
    """
    cursor.execute(query, (sex, ua_threshold, patient_id))
    result = cursor.fetchone()
    return {"boolean_value": result[0]}

# Endpoint to get laboratory dates with specific conditions
@app.get("/v1/bird/thrombosis_prediction/laboratory_dates", summary="Get laboratory dates with specific conditions")
async def get_laboratory_dates(patient_id: int = Query(..., description="Patient ID"),
                               got_min: int = Query(..., description="Minimum GOT value")):
    query = f"""
    SELECT Date
    FROM Laboratory
    WHERE ID = ? AND GOT >= ?
    """
    cursor.execute(query, (patient_id, got_min))
    result = cursor.fetchall()
    return {"dates": [row[0] for row in result]}

# Endpoint to get distinct patient sex and birthday with specific conditions
@app.get("/v1/bird/thrombosis_prediction/distinct_patient_info", summary="Get distinct patient sex and birthday with specific conditions")
async def get_distinct_patient_info(got_max: int = Query(..., description="Maximum GOT value"),
                                    year: str = Query(..., description="Year value")):
    query = f"""
    SELECT DISTINCT T1.SEX, T1.Birthday
    FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    WHERE T2.GOT < ? AND STRFTIME('%Y', T2.Date) = ?
    """
    cursor.execute(query, (got_max, year))
    result = cursor.fetchall()
    return {"patient_info": [{"sex": row[0], "birthday": row[1]} for row in result]}

# Endpoint to get distinct patient IDs with specific conditions
@app.get("/v1/bird/thrombosis_prediction/distinct_patient_ids_by_gpt", summary="Get distinct patient IDs with specific conditions by GPT")
async def get_distinct_patient_ids_by_gpt(sex: str = Query(..., description="Sex value"),
                                          gpt_min: int = Query(..., description="Minimum GPT value")):
    query = f"""
    SELECT DISTINCT T1.ID
    FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    WHERE T1.SEX = ? AND T2.GPT >= ?
    """
    cursor.execute(query, (sex, gpt_min))
    result = cursor.fetchall()
    return {"patient_ids": [row[0] for row in result]}

# Endpoint to get distinct diagnoses for patients with GPT > 60
@app.get("/v1/bird/thrombosis_prediction/diagnoses", summary="Get distinct diagnoses for patients with GPT > 60")
async def get_diagnoses(gpt_threshold: int = Query(..., description="GPT threshold value")):
    query = f"SELECT DISTINCT T1.Diagnosis FROM Patient AS T1 INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID WHERE T2.GPT > {gpt_threshold} ORDER BY T1.Birthday ASC"
    cursor.execute(query)
    results = cursor.fetchall()
    return results

# Endpoint to get average LDH for LDH < 500
@app.get("/v1/bird/thrombosis_prediction/average_ldh", summary="Get average LDH for LDH < 500")
async def get_average_ldh(ldh_threshold: int = Query(..., description="LDH threshold value")):
    query = f"SELECT AVG(LDH) FROM Laboratory WHERE LDH < {ldh_threshold}"
    cursor.execute(query)
    results = cursor.fetchall()
    return results

# Endpoint to get distinct patient IDs and age for LDH between 600 and 800
@app.get("/v1/bird/thrombosis_prediction/patient_age", summary="Get distinct patient IDs and age for LDH between 600 and 800")
async def get_patient_age(ldh_min: int = Query(..., description="Minimum LDH value"), ldh_max: int = Query(..., description="Maximum LDH value")):
    query = f"SELECT DISTINCT T1.ID, STRFTIME('%Y', CURRENT_TIMESTAMP) - STRFTIME('%Y', T1.Birthday) FROM Patient AS T1 INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID WHERE T2.LDH > {ldh_min} AND T2.LDH < {ldh_max}"
    cursor.execute(query)
    results = cursor.fetchall()
    return results

# Endpoint to get admission dates for patients with ALP < 300
@app.get("/v1/bird/thrombosis_prediction/admission_dates", summary="Get admission dates for patients with ALP < 300")
async def get_admission_dates(alp_threshold: int = Query(..., description="ALP threshold value")):
    query = f"SELECT T1.Admission FROM Patient AS T1 INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID WHERE T2.ALP < {alp_threshold}"
    cursor.execute(query)
    results = cursor.fetchall()
    return results

# Endpoint to get patient IDs and ALP status for a specific birthday
@app.get("/v1/bird/thrombosis_prediction/alp_status", summary="Get patient IDs and ALP status for a specific birthday")
async def get_alp_status(birthday: str = Query(..., description="Birthday in YYYY-MM-DD format")):
    query = f"SELECT T1.ID, CASE WHEN T2.ALP < 300 THEN 'normal' ELSE 'abNormal' END FROM Patient AS T1 INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID WHERE T1.Birthday = '{birthday}'"
    cursor.execute(query)
    results = cursor.fetchall()
    return results

# Endpoint to get distinct patient IDs, sex, and birthday for TP < 6.0
@app.get("/v1/bird/thrombosis_prediction/patient_details", summary="Get distinct patient IDs, sex, and birthday for TP < 6.0")
async def get_patient_details(tp_threshold: float = Query(..., description="TP threshold value")):
    query = f"SELECT DISTINCT T1.ID, T1.SEX, T1.Birthday FROM Patient AS T1 INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID WHERE T2.TP < {tp_threshold}"
    cursor.execute(query)
    results = cursor.fetchall()
    return results

# Endpoint to get TP difference for female patients with TP > 8.5
@app.get("/v1/bird/thrombosis_prediction/tp_difference", summary="Get TP difference for female patients with TP > 8.5")
async def get_tp_difference(tp_threshold: float = Query(..., description="TP threshold value")):
    query = f"SELECT T2.TP - 8.5 FROM Patient AS T1 INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID WHERE T1.SEX = 'F' AND T2.TP > {tp_threshold}"
    cursor.execute(query)
    results = cursor.fetchall()
    return results

# Endpoint to get distinct patient IDs for male patients with ALB <= 3.5 or ALB >= 5.5
@app.get("/v1/bird/thrombosis_prediction/patient_ids_alb", summary="Get distinct patient IDs for male patients with ALB <= 3.5 or ALB >= 5.5")
async def get_patient_ids_alb(alb_min: float = Query(..., description="Minimum ALB value"), alb_max: float = Query(..., description="Maximum ALB value")):
    query = f"SELECT DISTINCT T1.ID FROM Patient AS T1 INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID WHERE T1.SEX = 'M' AND (T2.ALB <= {alb_min} OR T2.ALB >= {alb_max}) ORDER BY T1.Birthday DESC"
    cursor.execute(query)
    results = cursor.fetchall()
    return results

# Endpoint to get ALB status for patients born in a specific year
@app.get("/v1/bird/thrombosis_prediction/alb_status", summary="Get ALB status for patients born in a specific year")
async def get_alb_status(birth_year: int = Query(..., description="Birth year")):
    query = f"SELECT CASE WHEN T2.ALB >= 3.5 AND T2.ALB <= 5.5 THEN 'normal' ELSE 'abnormal' END FROM Patient AS T1 INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID WHERE STRFTIME('%Y', T1.Birthday) = '{birth_year}'"
    cursor.execute(query)
    results = cursor.fetchall()
    return results

# Endpoint to get percentage of female patients with UA > 6.5
@app.get("/v1/bird/thrombosis_prediction/ua_percentage", summary="Get percentage of female patients with UA > 6.5")
async def get_ua_percentage(ua_threshold: float = Query(..., description="UA threshold value")):
    query = f"SELECT CAST(SUM(CASE WHEN T2.UA > {ua_threshold} THEN 1 ELSE 0 END) AS REAL) * 100 / COUNT(T1.ID) FROM Patient AS T1 INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID WHERE T1.SEX = 'F'"
    cursor.execute(query)
    results = cursor.fetchall()
    return results

# Endpoint to get average UA for specific conditions
@app.get("/v1/bird/thrombosis_prediction/average_ua", summary="Get average UA for specific conditions")
async def get_average_ua(ua_threshold_f: float = Query(..., description="UA threshold for females"), ua_threshold_m: float = Query(..., description="UA threshold for males")):
    query = f"SELECT AVG(T2.UA) FROM Patient AS T1 INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID WHERE (T2.UA < {ua_threshold_f} AND T1.SEX = 'F') OR (T2.UA < {ua_threshold_m} AND T1.SEX = 'M') AND T2.Date = (SELECT MAX(Date) FROM Laboratory)"
    cursor.execute(query)
    results = cursor.fetchall()
    return results

# Endpoint to get distinct patient IDs, sex, and birthday for UN = 29
@app.get("/v1/bird/thrombosis_prediction/patient_details_un", summary="Get distinct patient IDs, sex, and birthday for UN = 29")
async def get_patient_details_un(un_value: int = Query(..., description="UN value")):
    query = f"SELECT DISTINCT T1.ID, T1.SEX, T1.Birthday FROM Patient AS T1 INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID WHERE T2.UN = {un_value}"
    cursor.execute(query)
    results = cursor.fetchall()
    return results

# Endpoint to get distinct patient IDs, sex, and birthday for UN < 30 and diagnosis = 'RA'
@app.get("/v1/bird/thrombosis_prediction/patient_details_un_diagnosis", summary="Get distinct patient IDs, sex, and birthday for UN < 30 and diagnosis = 'RA'")
async def get_patient_details_un_diagnosis(un_threshold: int = Query(..., description="UN threshold value"), diagnosis: str = Query(..., description="Diagnosis")):
    query = f"SELECT DISTINCT T1.ID, T1.SEX, T1.Birthday FROM Patient AS T1 INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID WHERE T2.UN < {un_threshold} AND T1.Diagnosis = '{diagnosis}'"
    cursor.execute(query)
    results = cursor.fetchall()
    return results

# Endpoint to get count of male patients with CRE >= 1.5
@app.get("/v1/bird/thrombosis_prediction/patient_count_cre", summary="Get count of male patients with CRE >= 1.5")
async def get_patient_count_cre(cre_threshold: float = Query(..., description="CRE threshold value")):
    query = f"SELECT COUNT(T1.ID) FROM Patient AS T1 INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID WHERE T2.CRE >= {cre_threshold} AND T1.SEX = 'M'"
    cursor.execute(query)
    results = cursor.fetchall()
    return results

# Endpoint to get gender comparison for CRE >= 1.5
@app.get("/v1/bird/thrombosis_prediction/gender_comparison_cre", summary="Get gender comparison for CRE >= 1.5")
async def get_gender_comparison_cre(cre_threshold: float = Query(..., description="CRE threshold value")):
    query = f"SELECT CASE WHEN SUM(CASE WHEN T1.SEX = 'M' THEN 1 ELSE 0 END) > SUM(CASE WHEN T1.SEX = 'F' THEN 1 ELSE 0 END) THEN 'True' ELSE 'False' END FROM Patient AS T1 INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID WHERE T2.CRE >= {cre_threshold}"
    cursor.execute(query)
    results = cursor.fetchall()
    return results

# Endpoint to get patient details with highest T-BIL
@app.get("/v1/bird/thrombosis_prediction/highest_t_bil", summary="Get patient details with highest T-BIL")
async def get_highest_t_bil():
    query = "SELECT T2.`T-BIL`, T1.ID, T1.SEX, T1.Birthday FROM Patient AS T1 INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID ORDER BY T2.`T-BIL` DESC LIMIT 1"
    cursor.execute(query)
    results = cursor.fetchall()
    return results

# Endpoint to get patient IDs and sex for T-BIL >= 2.0
@app.get("/v1/bird/thrombosis_prediction/patient_ids_t_bil", summary="Get patient IDs and sex for T-BIL >= 2.0")
async def get_patient_ids_t_bil(t_bil_threshold: float = Query(..., description="T-BIL threshold value")):
    query = f"SELECT T1.ID, T1.SEX FROM Patient AS T1 INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID WHERE T2.`T-BIL` >= {t_bil_threshold} GROUP BY T1.SEX, T1.ID"
    cursor.execute(query)
    results = cursor.fetchall()
    return results

# Endpoint to get patient ID and T-CHO with highest T-CHO
@app.get("/v1/bird/thrombosis_prediction/highest_t_cho", summary="Get patient ID and T-CHO with highest T-CHO")
async def get_highest_t_cho():
    query = "SELECT T1.ID, T2.`T-CHO` FROM Patient AS T1 INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID ORDER BY T2.`T-CHO` DESC, T1.Birthday ASC LIMIT 1"
    cursor.execute(query)
    results = cursor.fetchall()
    return results

# Endpoint to get average age for male patients with T-CHO >= 250
@app.get("/v1/bird/thrombosis_prediction/average_age_t_cho", summary="Get average age for male patients with T-CHO >= 250")
async def get_average_age_t_cho(t_cho_threshold: int = Query(..., description="T-CHO threshold value")):
    query = f"SELECT AVG(STRFTIME('%Y', date('NOW')) - STRFTIME('%Y', T1.Birthday)) FROM Patient AS T1 INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID WHERE T2.`T-CHO` >= {t_cho_threshold} AND T1.SEX = 'M'"
    cursor.execute(query)
    results = cursor.fetchall()
    return results

# Endpoint to get patient IDs and diagnosis for TG > 300
@app.get("/v1/bird/thrombosis_prediction/patient_ids_tg", summary="Get patient IDs and diagnosis for TG > 300")
async def get_patient_ids_tg(tg_threshold: int = Query(..., description="TG threshold value")):
    query = f"SELECT T1.ID, T1.Diagnosis FROM Patient AS T1 INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID WHERE T2.TG > {tg_threshold}"
    cursor.execute(query)
    results = cursor.fetchall()
    return results
# Endpoint to get count of distinct patient IDs based on laboratory TG and age
@app.get("/v1/bird/thrombosis_prediction/patient_count_by_tg_and_age", summary="Get count of distinct patient IDs based on laboratory TG and age")
async def get_patient_count_by_tg_and_age(tg_threshold: int = Query(..., description="Threshold for TG value"), age_threshold: int = Query(..., description="Age threshold")):
    query = f"""
    SELECT COUNT(DISTINCT T1.ID)
    FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    WHERE T2.TG >= {tg_threshold} AND STRFTIME('%Y', CURRENT_TIMESTAMP) - STRFTIME('%Y', T1.Birthday) > {age_threshold}
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get distinct patient IDs based on laboratory CPK and admission status
@app.get("/v1/bird/thrombosis_prediction/patient_ids_by_cpk_and_admission", summary="Get distinct patient IDs based on laboratory CPK and admission status")
async def get_patient_ids_by_cpk_and_admission(cpk_threshold: int = Query(..., description="Threshold for CPK value"), admission_status: str = Query(..., description="Admission status")):
    query = f"""
    SELECT DISTINCT T1.ID
    FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    WHERE T2.CPK < {cpk_threshold} AND T1.Admission = '{admission_status}'
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get count of distinct patient IDs based on birth year range, sex, and laboratory CPK
@app.get("/v1/bird/thrombosis_prediction/patient_count_by_birth_year_sex_cpk", summary="Get count of distinct patient IDs based on birth year range, sex, and laboratory CPK")
async def get_patient_count_by_birth_year_sex_cpk(start_year: int = Query(..., description="Start year of birth year range"), end_year: int = Query(..., description="End year of birth year range"), sex: str = Query(..., description="Sex of the patient"), cpk_threshold: int = Query(..., description="Threshold for CPK value")):
    query = f"""
    SELECT COUNT(DISTINCT T1.ID)
    FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    WHERE STRFTIME('%Y', T1.Birthday) BETWEEN '{start_year}' AND '{end_year}' AND T1.SEX = '{sex}' AND T2.CPK >= {cpk_threshold}
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get distinct patient IDs, sex, and age based on laboratory GLU and T-CHO
@app.get("/v1/bird/thrombosis_prediction/patient_ids_by_glu_tcho", summary="Get distinct patient IDs, sex, and age based on laboratory GLU and T-CHO")
async def get_patient_ids_by_glu_tcho(glu_threshold: int = Query(..., description="Threshold for GLU value"), tcho_threshold: int = Query(..., description="Threshold for T-CHO value")):
    query = f"""
    SELECT DISTINCT T1.ID, T1.SEX, STRFTIME('%Y', CURRENT_TIMESTAMP) - STRFTIME('%Y', T1.Birthday)
    FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    WHERE T2.GLU >= {glu_threshold} AND T2.`T-CHO` < {tcho_threshold}
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get distinct patient IDs and GLU based on first date year and GLU threshold
@app.get("/v1/bird/thrombosis_prediction/patient_ids_by_first_date_glu", summary="Get distinct patient IDs and GLU based on first date year and GLU threshold")
async def get_patient_ids_by_first_date_glu(first_date_year: int = Query(..., description="Year of the first date"), glu_threshold: int = Query(..., description="Threshold for GLU value")):
    query = f"""
    SELECT DISTINCT T1.ID, T2.GLU
    FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    WHERE STRFTIME('%Y', T1.`First Date`) = '{first_date_year}' AND T2.GLU < {glu_threshold}
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get distinct patient IDs, sex, and birthday based on WBC range
@app.get("/v1/bird/thrombosis_prediction/patient_ids_by_wbc_range", summary="Get distinct patient IDs, sex, and birthday based on WBC range")
async def get_patient_ids_by_wbc_range(wbc_lower: float = Query(..., description="Lower threshold for WBC value"), wbc_upper: float = Query(..., description="Upper threshold for WBC value")):
    query = f"""
    SELECT DISTINCT T1.ID, T1.SEX, T1.Birthday
    FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    WHERE T2.WBC <= {wbc_lower} OR T2.WBC >= {wbc_upper}
    GROUP BY T1.SEX, T1.ID
    ORDER BY T1.Birthday ASC
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get distinct patient diagnosis, ID, and age based on RBC threshold
@app.get("/v1/bird/thrombosis_prediction/patient_diagnosis_by_rbc", summary="Get distinct patient diagnosis, ID, and age based on RBC threshold")
async def get_patient_diagnosis_by_rbc(rbc_threshold: float = Query(..., description="Threshold for RBC value")):
    query = f"""
    SELECT DISTINCT T1.Diagnosis, T1.ID, STRFTIME('%Y', CURRENT_TIMESTAMP) - STRFTIME('%Y', T1.Birthday)
    FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    WHERE T2.RBC < {rbc_threshold}
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get distinct patient IDs and admission based on sex, RBC range, and age threshold
@app.get("/v1/bird/thrombosis_prediction/patient_ids_by_sex_rbc_age", summary="Get distinct patient IDs and admission based on sex, RBC range, and age threshold")
async def get_patient_ids_by_sex_rbc_age(sex: str = Query(..., description="Sex of the patient"), rbc_lower: float = Query(..., description="Lower threshold for RBC value"), rbc_upper: float = Query(..., description="Upper threshold for RBC value"), age_threshold: int = Query(..., description="Age threshold")):
    query = f"""
    SELECT DISTINCT T1.ID, T1.Admission
    FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    WHERE T1.SEX = '{sex}' AND (T2.RBC <= {rbc_lower} OR T2.RBC >= {rbc_upper}) AND STRFTIME('%Y', CURRENT_TIMESTAMP) - STRFTIME('%Y', T1.Birthday) >= {age_threshold}
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get distinct patient IDs and sex based on HGB threshold and admission status
@app.get("/v1/bird/thrombosis_prediction/patient_ids_by_hgb_admission", summary="Get distinct patient IDs and sex based on HGB threshold and admission status")
async def get_patient_ids_by_hgb_admission(hgb_threshold: int = Query(..., description="Threshold for HGB value"), admission_status: str = Query(..., description="Admission status")):
    query = f"""
    SELECT DISTINCT T1.ID, T1.SEX
    FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    WHERE T2.HGB < {hgb_threshold} AND T1.Admission = '{admission_status}'
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get patient IDs and sex based on diagnosis, HGB range, and ordered by birthday
@app.get("/v1/bird/thrombosis_prediction/patient_ids_by_diagnosis_hgb", summary="Get patient IDs and sex based on diagnosis, HGB range, and ordered by birthday")
async def get_patient_ids_by_diagnosis_hgb(diagnosis: str = Query(..., description="Diagnosis of the patient"), hgb_lower: int = Query(..., description="Lower threshold for HGB value"), hgb_upper: int = Query(..., description="Upper threshold for HGB value")):
    query = f"""
    SELECT T1.ID, T1.SEX
    FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    WHERE T1.Diagnosis = '{diagnosis}' AND T2.HGB > {hgb_lower} AND T2.HGB < {hgb_upper}
    ORDER BY T1.Birthday ASC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get distinct patient IDs and age based on HCT threshold and count of laboratory records
@app.get("/v1/bird/thrombosis_prediction/patient_ids_by_hct_count", summary="Get distinct patient IDs and age based on HCT threshold and count of laboratory records")
async def get_patient_ids_by_hct_count(hct_threshold: int = Query(..., description="Threshold for HCT value"), count_threshold: int = Query(..., description="Count threshold for laboratory records")):
    query = f"""
    SELECT DISTINCT T1.ID, STRFTIME('%Y', CURRENT_TIMESTAMP) - STRFTIME('%Y', T1.Birthday)
    FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    WHERE T1.ID IN (
        SELECT ID
        FROM Laboratory
        WHERE HCT >= {hct_threshold}
        GROUP BY ID
        HAVING COUNT(ID) >= {count_threshold}
    )
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get average HCT based on HCT threshold and date year
@app.get("/v1/bird/thrombosis_prediction/average_hct_by_date", summary="Get average HCT based on HCT threshold and date year")
async def get_average_hct_by_date(hct_threshold: int = Query(..., description="Threshold for HCT value"), date_year: int = Query(..., description="Year of the date")):
    query = f"""
    SELECT AVG(T2.HCT)
    FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    WHERE T2.HCT < {hct_threshold} AND STRFTIME('%Y', T2.Date) = '{date_year}'
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get difference in count of PLT ranges
@app.get("/v1/bird/thrombosis_prediction/plt_count_difference", summary="Get difference in count of PLT ranges")
async def get_plt_count_difference():
    query = """
    SELECT SUM(CASE WHEN T2.PLT <= 100 THEN 1 ELSE 0 END) - SUM(CASE WHEN T2.PLT >= 400 THEN 1 ELSE 0 END)
    FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get distinct patient IDs based on PLT range, age threshold, and date year
@app.get("/v1/bird/thrombosis_prediction/patient_ids_by_plt_age_date", summary="Get distinct patient IDs based on PLT range, age threshold, and date year")
async def get_patient_ids_by_plt_age_date(plt_lower: int = Query(..., description="Lower threshold for PLT value"), plt_upper: int = Query(..., description="Upper threshold for PLT value"), age_threshold: int = Query(..., description="Age threshold"), date_year: int = Query(..., description="Year of the date")):
    query = f"""
    SELECT DISTINCT T1.ID
    FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    WHERE T2.PLT BETWEEN {plt_lower} AND {plt_upper} AND STRFTIME('%Y', T2.Date) - STRFTIME('%Y', T1.Birthday) < {age_threshold} AND STRFTIME('%Y', T2.Date) = '{date_year}'
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get percentage of patients with PT threshold and age threshold
@app.get("/v1/bird/thrombosis_prediction/pt_percentage_by_age", summary="Get percentage of patients with PT threshold and age threshold")
async def get_pt_percentage_by_age(pt_threshold: int = Query(..., description="Threshold for PT value"), age_threshold: int = Query(..., description="Age threshold")):
    query = f"""
    SELECT CAST(SUM(CASE WHEN T2.PT >= {pt_threshold} AND T1.SEX = 'F' THEN 1 ELSE 0 END) AS REAL) * 100 / COUNT(*)
    FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    WHERE STRFTIME('%Y', CURRENT_TIMESTAMP) - STRFTIME('%Y', T1.Birthday) > {age_threshold}
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get patient IDs based on first date year and PT threshold
@app.get("/v1/bird/thrombosis_prediction/patient_ids_by_first_date_pt", summary="Get patient IDs based on first date year and PT threshold")
async def get_patient_ids_by_first_date_pt(first_date_year: int = Query(..., description="Year of the first date"), pt_threshold: int = Query(..., description="Threshold for PT value")):
    query = f"""
    SELECT T1.ID
    FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    WHERE STRFTIME('%Y', T1.`First Date`) > '{first_date_year}' AND T2.PT < {pt_threshold}
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get count of patient IDs based on date and APTT threshold
@app.get("/v1/bird/thrombosis_prediction/patient_count_by_date_aptt", summary="Get count of patient IDs based on date and APTT threshold")
async def get_patient_count_by_date_aptt(date: str = Query(..., description="Date threshold"), aptt_threshold: int = Query(..., description="Threshold for APTT value")):
    query = f"""
    SELECT COUNT(T1.ID)
    FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    WHERE T2.Date > '{date}' AND T2.APTT >= {aptt_threshold}
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get count of distinct patient IDs based on thrombosis status and APTT threshold
@app.get("/v1/bird/thrombosis_prediction/patient_count_by_thrombosis_aptt", summary="Get count of distinct patient IDs based on thrombosis status and APTT threshold")
async def get_patient_count_by_thrombosis_aptt(thrombosis_status: int = Query(..., description="Thrombosis status"), aptt_threshold: int = Query(..., description="Threshold for APTT value")):
    query = f"""
    SELECT COUNT(DISTINCT T1.ID)
    FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    INNER JOIN Examination AS T3 ON T3.ID = T2.ID
    WHERE T3.Thrombosis = {thrombosis_status} AND T2.APTT > {aptt_threshold}
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get count of distinct patient IDs based on FG range, WBC range, and sex
@app.get("/v1/bird/thrombosis_prediction/patient_count_by_fg_wbc_sex", summary="Get count of distinct patient IDs based on FG range, WBC range, and sex")
async def get_patient_count_by_fg_wbc_sex(fg_lower: int = Query(..., description="Lower threshold for FG value"), fg_upper: int = Query(..., description="Upper threshold for FG value"), wbc_lower: float = Query(..., description="Lower threshold for WBC value"), wbc_upper: float = Query(..., description="Upper threshold for WBC value"), sex: str = Query(..., description="Sex of the patient")):
    query = f"""
    SELECT COUNT(DISTINCT T1.ID)
    FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    WHERE (T2.FG <= {fg_lower} OR T2.FG >= {fg_upper}) AND T2.WBC > {wbc_lower} AND T2.WBC < {wbc_upper} AND T1.SEX = '{sex}'
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get count of distinct patient IDs based on FG range and birthday threshold
@app.get("/v1/bird/thrombosis_prediction/patient_count_by_fg_birthday", summary="Get count of distinct patient IDs based on FG range and birthday threshold")
async def get_patient_count_by_fg_birthday(fg_lower: int = Query(..., description="Lower threshold for FG value"), fg_upper: int = Query(..., description="Upper threshold for FG value"), birthday_threshold: str = Query(..., description="Birthday threshold")):
    query = f"""
    SELECT COUNT(DISTINCT T1.ID)
    FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    WHERE (T2.FG <= {fg_lower} OR T2.FG >= {fg_upper}) AND T1.Birthday > '{birthday_threshold}'
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return result
# Endpoint to get diagnosis for patients with U-PRO >= 30
@app.get("/v1/bird/thrombosis_prediction/diagnosis_by_u_pro", summary="Get diagnosis for patients with U-PRO >= 30")
async def get_diagnosis_by_u_pro(u_pro: int = Query(..., description="U-PRO value")):
    query = f"SELECT T1.Diagnosis FROM Patient AS T1 INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID WHERE T2.`U-PRO` >= ?"
    cursor.execute(query, (u_pro,))
    return cursor.fetchall()

# Endpoint to get distinct patient IDs with U-PRO between 0 and 30 and diagnosis SLE
@app.get("/v1/bird/thrombosis_prediction/patient_ids_by_u_pro_and_diagnosis", summary="Get distinct patient IDs with U-PRO between 0 and 30 and diagnosis SLE")
async def get_patient_ids_by_u_pro_and_diagnosis(u_pro_min: int = Query(..., description="Minimum U-PRO value"),
                                                 u_pro_max: int = Query(..., description="Maximum U-PRO value"),
                                                 diagnosis: str = Query(..., description="Diagnosis")):
    query = f"SELECT DISTINCT T1.ID FROM Patient AS T1 INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID WHERE T2.`U-PRO` > ? AND T2.`U-PRO` < ? AND T1.Diagnosis = ?"
    cursor.execute(query, (u_pro_min, u_pro_max, diagnosis))
    return cursor.fetchall()

# Endpoint to get count of distinct patient IDs with IGG >= 2000
@app.get("/v1/bird/thrombosis_prediction/count_distinct_patients_by_igg", summary="Get count of distinct patient IDs with IGG >= 2000")
async def get_count_distinct_patients_by_igg(igg: int = Query(..., description="IGG value")):
    query = f"SELECT COUNT(DISTINCT T1.ID) FROM Patient AS T1 INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID INNER JOIN Examination AS T3 ON T3.ID = T2.ID WHERE T2.IGG >= ?"
    cursor.execute(query, (igg,))
    return cursor.fetchall()

# Endpoint to get count of patient IDs with IGG between 900 and 2000 and non-null symptoms
@app.get("/v1/bird/thrombosis_prediction/count_patients_by_igg_and_symptoms", summary="Get count of patient IDs with IGG between 900 and 2000 and non-null symptoms")
async def get_count_patients_by_igg_and_symptoms(igg_min: int = Query(..., description="Minimum IGG value"),
                                                 igg_max: int = Query(..., description="Maximum IGG value")):
    query = f"SELECT COUNT(T1.ID) FROM Patient AS T1 INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID INNER JOIN Examination AS T3 ON T3.ID = T2.ID WHERE T2.IGG BETWEEN ? AND ? AND T3.Symptoms IS NOT NULL"
    cursor.execute(query, (igg_min, igg_max))
    return cursor.fetchall()

# Endpoint to get diagnosis for patients with IGA between 80 and 500 ordered by IGA descending
@app.get("/v1/bird/thrombosis_prediction/diagnosis_by_iga", summary="Get diagnosis for patients with IGA between 80 and 500 ordered by IGA descending")
async def get_diagnosis_by_iga(iga_min: int = Query(..., description="Minimum IGA value"),
                               iga_max: int = Query(..., description="Maximum IGA value")):
    query = f"SELECT patientData.Diagnosis FROM Patient AS patientData INNER JOIN Laboratory AS labData ON patientData.ID = labData.ID WHERE labData.IGA BETWEEN ? AND ? ORDER BY labData.IGA DESC LIMIT 1"
    cursor.execute(query, (iga_min, iga_max))
    return cursor.fetchall()

# Endpoint to get count of patient IDs with IGA between 80 and 500 and first date after 1990
@app.get("/v1/bird/thrombosis_prediction/count_patients_by_iga_and_date", summary="Get count of patient IDs with IGA between 80 and 500 and first date after 1990")
async def get_count_patients_by_iga_and_date(iga_min: int = Query(..., description="Minimum IGA value"),
                                             iga_max: int = Query(..., description="Maximum IGA value"),
                                             year: int = Query(..., description="Year")):
    query = f"SELECT COUNT(T1.ID) FROM Patient AS T1 INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID WHERE T2.IGA BETWEEN ? AND ? AND strftime('%Y', T1.`First Date`) > ?"
    cursor.execute(query, (iga_min, iga_max, year))
    return cursor.fetchall()

# Endpoint to get diagnosis for patients with IGM not between 40 and 400 grouped by diagnosis
@app.get("/v1/bird/thrombosis_prediction/diagnosis_by_igm", summary="Get diagnosis for patients with IGM not between 40 and 400 grouped by diagnosis")
async def get_diagnosis_by_igm(igm_min: int = Query(..., description="Minimum IGM value"),
                               igm_max: int = Query(..., description="Maximum IGM value")):
    query = f"SELECT T1.Diagnosis FROM Patient AS T1 INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID WHERE T2.IGM NOT BETWEEN ? AND ? GROUP BY T1.Diagnosis ORDER BY COUNT(T1.Diagnosis) DESC LIMIT 1"
    cursor.execute(query, (igm_min, igm_max))
    return cursor.fetchall()

# Endpoint to get count of patient IDs with CRP = '+' and null description
@app.get("/v1/bird/thrombosis_prediction/count_patients_by_crp_and_description", summary="Get count of patient IDs with CRP = '+' and null description")
async def get_count_patients_by_crp_and_description(crp: str = Query(..., description="CRP value")):
    query = f"SELECT COUNT(T1.ID) FROM Patient AS T1 INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID WHERE T2.CRP = ? AND T1.Description IS NULL"
    cursor.execute(query, (crp,))
    return cursor.fetchall()

# Endpoint to get count of distinct patient IDs with CRE >= 1.5 and age < 70
@app.get("/v1/bird/thrombosis_prediction/count_distinct_patients_by_cre_and_age", summary="Get count of distinct patient IDs with CRE >= 1.5 and age < 70")
async def get_count_distinct_patients_by_cre_and_age(cre: float = Query(..., description="CRE value"),
                                                     age: int = Query(..., description="Age")):
    query = f"SELECT COUNT(DISTINCT T1.ID) FROM Patient AS T1 INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID WHERE T2.CRE >= ? AND STRFTIME('%Y', Date('now')) - STRFTIME('%Y', T1.Birthday) < ?"
    cursor.execute(query, (cre, age))
    return cursor.fetchall()

# Endpoint to get count of distinct patient IDs with RA = '-' or '+-' and KCT = '+'
@app.get("/v1/bird/thrombosis_prediction/count_distinct_patients_by_ra_and_kct", summary="Get count of distinct patient IDs with RA = '-' or '+-' and KCT = '+'")
async def get_count_distinct_patients_by_ra_and_kct(ra: str = Query(..., description="RA value"),
                                                    kct: str = Query(..., description="KCT value")):
    query = f"SELECT COUNT(DISTINCT T1.ID) FROM Patient AS T1 INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID INNER JOIN Examination AS T3 ON T3.ID = T2.ID WHERE (T2.RA = ? OR T2.RA = ?) AND T3.KCT = ?"
    cursor.execute(query, (ra, ra, kct))
    return cursor.fetchall()

# Endpoint to get diagnosis for patients with RA = '-' or '+-' and birthday after 1985-01-01
@app.get("/v1/bird/thrombosis_prediction/diagnosis_by_ra_and_birthday", summary="Get diagnosis for patients with RA = '-' or '+-' and birthday after 1985-01-01")
async def get_diagnosis_by_ra_and_birthday(ra: str = Query(..., description="RA value"),
                                           birthday: str = Query(..., description="Birthday")):
    query = f"SELECT T1.Diagnosis FROM Patient AS T1 INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID WHERE (T2.RA = ? OR T2.RA = ?) AND T1.Birthday > ?"
    cursor.execute(query, (ra, ra, birthday))
    return cursor.fetchall()

# Endpoint to get patient IDs with RF < 20 and age > 60
@app.get("/v1/bird/thrombosis_prediction/patient_ids_by_rf_and_age", summary="Get patient IDs with RF < 20 and age > 60")
async def get_patient_ids_by_rf_and_age(rf: int = Query(..., description="RF value"),
                                        age: int = Query(..., description="Age")):
    query = f"SELECT T1.ID FROM Patient AS T1 INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID WHERE T2.RF < ? AND STRFTIME('%Y', DATE('now')) - STRFTIME('%Y', T1.Birthday) > ?"
    cursor.execute(query, (rf, age))
    return cursor.fetchall()

# Endpoint to get count of distinct patient IDs with RF < 20 and Thrombosis = 0
@app.get("/v1/bird/thrombosis_prediction/count_distinct_patients_by_rf_and_thrombosis", summary="Get count of distinct patient IDs with RF < 20 and Thrombosis = 0")
async def get_count_distinct_patients_by_rf_and_thrombosis(rf: int = Query(..., description="RF value"),
                                                           thrombosis: int = Query(..., description="Thrombosis value")):
    query = f"SELECT COUNT(DISTINCT T1.ID) FROM Examination AS T1 INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID WHERE T2.RF < ? AND T1.Thrombosis = ?"
    cursor.execute(query, (rf, thrombosis))
    return cursor.fetchall()

# Endpoint to get count of distinct patient IDs with C3 > 35 and ANA Pattern = 'P'
@app.get("/v1/bird/thrombosis_prediction/count_distinct_patients_by_c3_and_ana_pattern", summary="Get count of distinct patient IDs with C3 > 35 and ANA Pattern = 'P'")
async def get_count_distinct_patients_by_c3_and_ana_pattern(c3: int = Query(..., description="C3 value"),
                                                            ana_pattern: str = Query(..., description="ANA Pattern")):
    query = f"SELECT COUNT(DISTINCT T1.ID) FROM Examination AS T1 INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID WHERE T2.C3 > ? AND T1.`ANA Pattern` = ?"
    cursor.execute(query, (c3, ana_pattern))
    return cursor.fetchall()

# Endpoint to get distinct patient IDs with HCT >= 52 or HCT <= 29 ordered by aCL IgA descending
@app.get("/v1/bird/thrombosis_prediction/distinct_patient_ids_by_hct_and_acl_iga", summary="Get distinct patient IDs with HCT >= 52 or HCT <= 29 ordered by aCL IgA descending")
async def get_distinct_patient_ids_by_hct_and_acl_iga(hct_min: int = Query(..., description="Minimum HCT value"),
                                                      hct_max: int = Query(..., description="Maximum HCT value")):
    query = f"SELECT DISTINCT T1.ID FROM Patient AS T1 INNER JOIN Examination AS T2 ON T1.ID = T2.ID INNER JOIN Laboratory AS T3 on T1.ID = T3.ID WHERE (T3.HCT >= ? OR T3.HCT <= ?) ORDER BY T2.`aCL IgA` DESC LIMIT 1"
    cursor.execute(query, (hct_min, hct_max))
    return cursor.fetchall()

# Endpoint to get count of distinct patient IDs with C4 > 10 and diagnosis APS
@app.get("/v1/bird/thrombosis_prediction/count_distinct_patients_by_c4_and_diagnosis", summary="Get count of distinct patient IDs with C4 > 10 and diagnosis APS")
async def get_count_distinct_patients_by_c4_and_diagnosis(c4: int = Query(..., description="C4 value"),
                                                          diagnosis: str = Query(..., description="Diagnosis")):
    query = f"SELECT COUNT(DISTINCT T1.ID) FROM Patient AS T1 INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID WHERE T2.C4 > ? AND T1.Diagnosis = ?"
    cursor.execute(query, (c4, diagnosis))
    return cursor.fetchall()

# Endpoint to get count of distinct patient IDs with RNP = 'negative' or '0' and admission '+'
@app.get("/v1/bird/thrombosis_prediction/count_distinct_patients_by_rnp_and_admission", summary="Get count of distinct patient IDs with RNP = 'negative' or '0' and admission '+'")
async def get_count_distinct_patients_by_rnp_and_admission(rnp: str = Query(..., description="RNP value"),
                                                           admission: str = Query(..., description="Admission value")):
    query = f"SELECT COUNT(DISTINCT T1.ID) FROM Patient AS T1 INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID WHERE (T2.RNP = ? OR T2.RNP = ?) AND T1.Admission = ?"
    cursor.execute(query, (rnp, rnp, admission))
    return cursor.fetchall()

# Endpoint to get patient birthday with RNP != '-' or '+-' ordered by birthday descending
@app.get("/v1/bird/thrombosis_prediction/patient_birthday_by_rnp", summary="Get patient birthday with RNP != '-' or '+-' ordered by birthday descending")
async def get_patient_birthday_by_rnp(rnp: str = Query(..., description="RNP value")):
    query = f"SELECT T1.Birthday FROM Patient AS T1 INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID WHERE T2.RNP != ? OR T2.RNP != ? ORDER BY T1.Birthday DESC LIMIT 1"
    cursor.execute(query, (rnp, rnp))
    return cursor.fetchall()

# Endpoint to get count of patient IDs with SM in ('negative','0') and Thrombosis = 0
@app.get("/v1/bird/thrombosis_prediction/count_patients_by_sm_and_thrombosis", summary="Get count of patient IDs with SM in ('negative','0') and Thrombosis = 0")
async def get_count_patients_by_sm_and_thrombosis(sm: str = Query(..., description="SM value"),
                                                  thrombosis: int = Query(..., description="Thrombosis value")):
    query = f"SELECT COUNT(T1.ID) FROM Examination AS T1 INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID WHERE T2.SM IN (?, ?) AND T1.Thrombosis = ?"
    cursor.execute(query, (sm, sm, thrombosis))
    return cursor.fetchall()

# Endpoint to get patient IDs with SM not in ('negative','0') ordered by birthday descending
@app.get("/v1/bird/thrombosis_prediction/patient_ids_by_sm", summary="Get patient IDs with SM not in ('negative','0') ordered by birthday descending")
async def get_patient_ids_by_sm(sm: str = Query(..., description="SM value"),
                                limit: int = Query(..., description="Limit")):
    query = f"SELECT T1.ID FROM Patient AS T1 INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID WHERE T2.SM NOT IN (?, ?) ORDER BY T1.Birthday DESC LIMIT ?"
    cursor.execute(query, (sm, sm, limit))
    return cursor.fetchall()

# Endpoint to get patient IDs based on laboratory results
@app.get("/v1/bird/thrombosis_prediction/patient_ids_by_lab_results", summary="Get patient IDs based on laboratory results")
async def get_patient_ids_by_lab_results(sc170: str = Query(..., description="SC170 value"), date: str = Query(..., description="Date in YYYY-MM-DD format")):
    query = """
    SELECT T1.ID FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    WHERE T2.SC170 IN (?, ?) AND T2.Date > ?
    """
    cursor.execute(query, (sc170, '0', date))
    results = cursor.fetchall()
    return results

# Endpoint to get count of distinct patient IDs based on multiple conditions
@app.get("/v1/bird/thrombosis_prediction/count_distinct_patient_ids", summary="Get count of distinct patient IDs based on multiple conditions")
async def get_count_distinct_patient_ids(sc170: str = Query(..., description="SC170 value"), sex: str = Query(..., description="Sex of the patient")):
    query = """
    SELECT COUNT(DISTINCT T1.ID) FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    INNER JOIN Examination AS T3 ON T3.ID = T2.ID
    WHERE (T2.SC170 = ? OR T2.SC170 = ?) AND T1.SEX = ? AND T3.Symptoms IS NULL
    """
    cursor.execute(query, (sc170, '0', sex))
    results = cursor.fetchall()
    return results

# Endpoint to get count of distinct patient IDs based on SSA and date
@app.get("/v1/bird/thrombosis_prediction/count_distinct_patient_ids_by_ssa", summary="Get count of distinct patient IDs based on SSA and date")
async def get_count_distinct_patient_ids_by_ssa(ssa: str = Query(..., description="SSA value"), year: str = Query(..., description="Year")):
    query = """
    SELECT COUNT(DISTINCT T1.ID) FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    WHERE T2.SSA IN (?, ?) AND STRFTIME('%Y', T2.Date) < ?
    """
    cursor.execute(query, (ssa, '0', year))
    results = cursor.fetchall()
    return results

# Endpoint to get patient ID based on first date and SSA
@app.get("/v1/bird/thrombosis_prediction/patient_id_by_first_date_and_ssa", summary="Get patient ID based on first date and SSA")
async def get_patient_id_by_first_date_and_ssa():
    query = """
    SELECT T1.ID FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    WHERE T1.`First Date` IS NOT NULL AND T2.SSA NOT IN ('negative', '0')
    ORDER BY T1.`First Date` ASC LIMIT 1
    """
    cursor.execute(query)
    results = cursor.fetchall()
    return results

# Endpoint to get count of distinct patient IDs based on SSB and diagnosis
@app.get("/v1/bird/thrombosis_prediction/count_distinct_patient_ids_by_ssb_and_diagnosis", summary="Get count of distinct patient IDs based on SSB and diagnosis")
async def get_count_distinct_patient_ids_by_ssb_and_diagnosis(ssb: str = Query(..., description="SSB value"), diagnosis: str = Query(..., description="Diagnosis")):
    query = """
    SELECT COUNT(DISTINCT T1.ID) FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    WHERE T2.SSB = ? OR T2.SSB = ? AND T1.Diagnosis = ?
    """
    cursor.execute(query, (ssb, '0', diagnosis))
    results = cursor.fetchall()
    return results

# Endpoint to get count of distinct patient IDs based on SSB and symptoms
@app.get("/v1/bird/thrombosis_prediction/count_distinct_patient_ids_by_ssb_and_symptoms", summary="Get count of distinct patient IDs based on SSB and symptoms")
async def get_count_distinct_patient_ids_by_ssb_and_symptoms(ssb: str = Query(..., description="SSB value")):
    query = """
    SELECT COUNT(DISTINCT T1.ID) FROM Examination AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    WHERE T2.SSB = ? OR T2.SSB = ? AND T1.Symptoms IS NOT NULL
    """
    cursor.execute(query, (ssb, '0'))
    results = cursor.fetchall()
    return results

# Endpoint to get count of distinct patient IDs based on CENTROMEA, SSB, and sex
@app.get("/v1/bird/thrombosis_prediction/count_distinct_patient_ids_by_centromea_ssb_sex", summary="Get count of distinct patient IDs based on CENTROMEA, SSB, and sex")
async def get_count_distinct_patient_ids_by_centromea_ssb_sex(centromea: str = Query(..., description="CENTROMEA value"), ssb: str = Query(..., description="SSB value"), sex: str = Query(..., description="Sex of the patient")):
    query = """
    SELECT COUNT(DISTINCT T1.ID) FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    WHERE T2.CENTROMEA IN (?, ?) AND T2.SSB IN (?, ?) AND T1.SEX = ?
    """
    cursor.execute(query, (centromea, '0', ssb, '0', sex))
    results = cursor.fetchall()
    return results

# Endpoint to get distinct diagnoses based on DNA
@app.get("/v1/bird/thrombosis_prediction/distinct_diagnoses_by_dna", summary="Get distinct diagnoses based on DNA")
async def get_distinct_diagnoses_by_dna(dna: int = Query(..., description="DNA value")):
    query = """
    SELECT DISTINCT(T1.Diagnosis) FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    WHERE T2.DNA >= ?
    """
    cursor.execute(query, (dna,))
    results = cursor.fetchall()
    return results

# Endpoint to get count of distinct patient IDs based on DNA and description
@app.get("/v1/bird/thrombosis_prediction/count_distinct_patient_ids_by_dna_and_description", summary="Get count of distinct patient IDs based on DNA and description")
async def get_count_distinct_patient_ids_by_dna_and_description(dna: int = Query(..., description="DNA value")):
    query = """
    SELECT COUNT(DISTINCT T1.ID) FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    WHERE T2.DNA < ? AND T1.Description IS NULL
    """
    cursor.execute(query, (dna,))
    results = cursor.fetchall()
    return results

# Endpoint to get count of patient IDs based on IGG and admission
@app.get("/v1/bird/thrombosis_prediction/count_patient_ids_by_igg_and_admission", summary="Get count of patient IDs based on IGG and admission")
async def get_count_patient_ids_by_igg_and_admission(igg_min: int = Query(..., description="Minimum IGG value"), igg_max: int = Query(..., description="Maximum IGG value"), admission: str = Query(..., description="Admission status")):
    query = """
    SELECT COUNT(T1.ID) FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    WHERE T2.IGG > ? AND T2.IGG < ? AND T1.Admission = ?
    """
    cursor.execute(query, (igg_min, igg_max, admission))
    results = cursor.fetchall()
    return results

# Endpoint to get ratio of SLE diagnoses based on GOT
@app.get("/v1/bird/thrombosis_prediction/ratio_sle_diagnoses_by_got", summary="Get ratio of SLE diagnoses based on GOT")
async def get_ratio_sle_diagnoses_by_got(got: int = Query(..., description="GOT value")):
    query = """
    SELECT COUNT(CASE WHEN T1.Diagnosis LIKE '%SLE%' THEN T1.ID ELSE 0 END) / COUNT(T1.ID) FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    WHERE T2.`GOT` >= ?
    """
    cursor.execute(query, (got,))
    results = cursor.fetchall()
    return results

# Endpoint to get count of patient IDs based on GOT and sex
@app.get("/v1/bird/thrombosis_prediction/count_patient_ids_by_got_and_sex", summary="Get count of patient IDs based on GOT and sex")
async def get_count_patient_ids_by_got_and_sex(got: int = Query(..., description="GOT value"), sex: str = Query(..., description="Sex of the patient")):
    query = """
    SELECT COUNT(T1.ID) FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    WHERE T2.GOT < ? AND T1.SEX = ?
    """
    cursor.execute(query, (got, sex))
    results = cursor.fetchall()
    return results

# Endpoint to get patient birthday based on GOT
@app.get("/v1/bird/thrombosis_prediction/patient_birthday_by_got", summary="Get patient birthday based on GOT")
async def get_patient_birthday_by_got(got: int = Query(..., description="GOT value")):
    query = """
    SELECT T1.Birthday FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    WHERE T2.GOT >= ?
    ORDER BY T1.Birthday DESC LIMIT 1
    """
    cursor.execute(query, (got,))
    results = cursor.fetchall()
    return results

# Endpoint to get patient birthdays based on GPT
@app.get("/v1/bird/thrombosis_prediction/patient_birthdays_by_gpt", summary="Get patient birthdays based on GPT")
async def get_patient_birthdays_by_gpt(gpt: int = Query(..., description="GPT value")):
    query = """
    SELECT T1.Birthday FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    WHERE T2.GPT < ?
    ORDER BY T2.GPT DESC LIMIT 3
    """
    cursor.execute(query, (gpt,))
    results = cursor.fetchall()
    return results

# Endpoint to get count of patient IDs based on GOT and sex
@app.get("/v1/bird/thrombosis_prediction/count_patient_ids_by_got_and_sex_v2", summary="Get count of patient IDs based on GOT and sex")
async def get_count_patient_ids_by_got_and_sex_v2(got: int = Query(..., description="GOT value"), sex: str = Query(..., description="Sex of the patient")):
    query = """
    SELECT COUNT(T1.ID) FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    WHERE T2.GOT < ? AND T1.SEX = ?
    """
    cursor.execute(query, (got, sex))
    results = cursor.fetchall()
    return results

# Endpoint to get patient first date based on LDH
@app.get("/v1/bird/thrombosis_prediction/patient_first_date_by_ldh", summary="Get patient first date based on LDH")
async def get_patient_first_date_by_ldh(ldh: int = Query(..., description="LDH value")):
    query = """
    SELECT T1.`First Date` FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    WHERE T2.LDH < ?
    ORDER BY T2.LDH ASC LIMIT 1
    """
    cursor.execute(query, (ldh,))
    results = cursor.fetchall()
    return results

# Endpoint to get patient first date based on LDH
@app.get("/v1/bird/thrombosis_prediction/patient_first_date_by_ldh_v2", summary="Get patient first date based on LDH")
async def get_patient_first_date_by_ldh_v2(ldh: int = Query(..., description="LDH value")):
    query = """
    SELECT T1.`First Date` FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    WHERE T2.LDH >= ?
    ORDER BY T1.`First Date` DESC LIMIT 1
    """
    cursor.execute(query, (ldh,))
    results = cursor.fetchall()
    return results

# Endpoint to get count of patient IDs based on ALP and admission
@app.get("/v1/bird/thrombosis_prediction/count_patient_ids_by_alp_and_admission", summary="Get count of patient IDs based on ALP and admission")
async def get_count_patient_ids_by_alp_and_admission(alp: int = Query(..., description="ALP value"), admission: str = Query(..., description="Admission status")):
    query = """
    SELECT COUNT(T1.ID) FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    WHERE T2.ALP >= ? AND T1.Admission = ?
    """
    cursor.execute(query, (alp, admission))
    results = cursor.fetchall()
    return results

# Endpoint to get count of patient IDs based on ALP and admission
@app.get("/v1/bird/thrombosis_prediction/count_patient_ids_by_alp_and_admission_v2", summary="Get count of patient IDs based on ALP and admission")
async def get_count_patient_ids_by_alp_and_admission_v2(alp: int = Query(..., description="ALP value"), admission: str = Query(..., description="Admission status")):
    query = """
    SELECT COUNT(T1.ID) FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    WHERE T2.ALP < ? AND T1.Admission = ?
    """
    cursor.execute(query, (alp, admission))
    results = cursor.fetchall()
    return results

# Endpoint to get diagnoses based on TP
@app.get("/v1/bird/thrombosis_prediction/diagnoses_by_tp", summary="Get diagnoses based on TP")
async def get_diagnoses_by_tp(tp: float = Query(..., description="TP value")):
    query = """
    SELECT T1.Diagnosis FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    WHERE T2.TP < ?
    """
    cursor.execute(query, (tp,))
    results = cursor.fetchall()
    return results

# Endpoint to get count of patients with specific diagnosis and TP range
@app.get("/v1/bird/thrombosis_prediction/patient_count_by_diagnosis_tp", summary="Get count of patients with specific diagnosis and TP range")
async def get_patient_count_by_diagnosis_tp(diagnosis: str = Query(..., description="Diagnosis of the patient"), tp_min: float = Query(..., description="Minimum TP value"), tp_max: float = Query(..., description="Maximum TP value")):
    query = f"SELECT COUNT(T1.ID) FROM Patient AS T1 INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID WHERE T1.Diagnosis = ? AND T2.TP > ? AND T2.TP < ?"
    cursor.execute(query, (diagnosis, tp_min, tp_max))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get date from laboratory with specific ALB range
@app.get("/v1/bird/thrombosis_prediction/laboratory_date_by_alb", summary="Get date from laboratory with specific ALB range")
async def get_laboratory_date_by_alb(alb_min: float = Query(..., description="Minimum ALB value"), alb_max: float = Query(..., description="Maximum ALB value")):
    query = f"SELECT Date FROM Laboratory WHERE ALB > ? AND ALB < ? ORDER BY ALB DESC LIMIT 1"
    cursor.execute(query, (alb_min, alb_max))
    result = cursor.fetchone()
    return {"date": result[0]}

# Endpoint to get count of patients with specific sex, ALB range, and TP range
@app.get("/v1/bird/thrombosis_prediction/patient_count_by_sex_alb_tp", summary="Get count of patients with specific sex, ALB range, and TP range")
async def get_patient_count_by_sex_alb_tp(sex: str = Query(..., description="Sex of the patient"), alb_min: float = Query(..., description="Minimum ALB value"), alb_max: float = Query(..., description="Maximum ALB value"), tp_min: float = Query(..., description="Minimum TP value"), tp_max: float = Query(..., description="Maximum TP value")):
    query = f"SELECT COUNT(T1.ID) FROM Patient AS T1 INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID WHERE T1.SEX = ? AND T2.ALB > ? AND T2.ALB < ? AND T2.TP BETWEEN ? AND ?"
    cursor.execute(query, (sex, alb_min, alb_max, tp_min, tp_max))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get aCL IgG, aCL IgM, aCL IgA from examination with specific sex and UA range
@app.get("/v1/bird/thrombosis_prediction/examination_acl_by_sex_ua", summary="Get aCL IgG, aCL IgM, aCL IgA from examination with specific sex and UA range")
async def get_examination_acl_by_sex_ua(sex: str = Query(..., description="Sex of the patient"), ua_min: float = Query(..., description="Minimum UA value")):
    query = f"SELECT T3.`aCL IgG`, T3.`aCL IgM`, T3.`aCL IgA` FROM Patient AS T1 INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID INNER JOIN Examination AS T3 ON T3.ID = T2.ID WHERE T1.SEX = ? AND T2.UA > ? ORDER BY T2.UA DESC LIMIT 1"
    cursor.execute(query, (sex, ua_min))
    result = cursor.fetchone()
    return {"aCL IgG": result[0], "aCL IgM": result[1], "aCL IgA": result[2]}

# Endpoint to get ANA from examination with specific CRE range
@app.get("/v1/bird/thrombosis_prediction/examination_ana_by_cre", summary="Get ANA from examination with specific CRE range")
async def get_examination_ana_by_cre(cre_max: float = Query(..., description="Maximum CRE value")):
    query = f"SELECT T2.ANA FROM Patient AS T1 INNER JOIN Examination AS T2 ON T1.ID = T2.ID INNER JOIN Laboratory AS T3 ON T1.ID = T3.ID WHERE T3.CRE < ? ORDER BY T2.ANA DESC LIMIT 1"
    cursor.execute(query, (cre_max,))
    result = cursor.fetchone()
    return {"ANA": result[0]}

# Endpoint to get ID from examination with specific CRE range
@app.get("/v1/bird/thrombosis_prediction/examination_id_by_cre", summary="Get ID from examination with specific CRE range")
async def get_examination_id_by_cre(cre_max: float = Query(..., description="Maximum CRE value")):
    query = f"SELECT T2.ID FROM Laboratory AS T1 INNER JOIN Examination AS T2 ON T1.ID = T2.ID WHERE T1.CRE < ? ORDER BY T2.`aCL IgA` DESC LIMIT 1"
    cursor.execute(query, (cre_max,))
    result = cursor.fetchone()
    return {"ID": result[0]}

# Endpoint to get count of patients with specific T-BIL range and ANA pattern
@app.get("/v1/bird/thrombosis_prediction/patient_count_by_tbil_ana_pattern", summary="Get count of patients with specific T-BIL range and ANA pattern")
async def get_patient_count_by_tbil_ana_pattern(tbil_min: float = Query(..., description="Minimum T-BIL value"), ana_pattern: str = Query(..., description="ANA pattern")):
    query = f"SELECT COUNT(T1.ID) FROM Patient AS T1 INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID INNER JOIN Examination AS T3 ON T1.ID = T3.ID WHERE T2.`T-BIL` >= ? AND T3.`ANA Pattern` LIKE ?"
    cursor.execute(query, (tbil_min, f"%{ana_pattern}%"))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get ANA from examination with specific T-BIL range
@app.get("/v1/bird/thrombosis_prediction/examination_ana_by_tbil", summary="Get ANA from examination with specific T-BIL range")
async def get_examination_ana_by_tbil(tbil_max: float = Query(..., description="Maximum T-BIL value")):
    query = f"SELECT T3.ANA FROM Patient AS T1 INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID INNER JOIN Examination AS T3 ON T1.ID = T3.ID WHERE T2.`T-BIL` < ? ORDER BY T2.`T-BIL` DESC LIMIT 1"
    cursor.execute(query, (tbil_max,))
    result = cursor.fetchone()
    return {"ANA": result[0]}

# Endpoint to get count of patients with specific T-CHO range and KCT value
@app.get("/v1/bird/thrombosis_prediction/patient_count_by_tcho_kct", summary="Get count of patients with specific T-CHO range and KCT value")
async def get_patient_count_by_tcho_kct(tcho_min: float = Query(..., description="Minimum T-CHO value"), kct: str = Query(..., description="KCT value")):
    query = f"SELECT COUNT(T1.ID) FROM Patient AS T1 INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID INNER JOIN Examination AS T3 ON T1.ID = T3.ID WHERE T2.`T-CHO` >= ? AND T3.KCT = ?"
    cursor.execute(query, (tcho_min, kct))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get count of patients with specific ANA pattern and T-CHO range
@app.get("/v1/bird/thrombosis_prediction/patient_count_by_ana_pattern_tcho", summary="Get count of patients with specific ANA pattern and T-CHO range")
async def get_patient_count_by_ana_pattern_tcho(ana_pattern: str = Query(..., description="ANA pattern"), tcho_max: float = Query(..., description="Maximum T-CHO value")):
    query = f"SELECT COUNT(T1.ID) FROM Patient AS T1 INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID INNER JOIN Examination AS T3 ON T1.ID = T3.ID WHERE T3.`ANA Pattern` = ? AND T2.`T-CHO` < ?"
    cursor.execute(query, (ana_pattern, tcho_max))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get count of examinations with specific TG range and non-null symptoms
@app.get("/v1/bird/thrombosis_prediction/examination_count_by_tg_symptoms", summary="Get count of examinations with specific TG range and non-null symptoms")
async def get_examination_count_by_tg_symptoms(tg_max: float = Query(..., description="Maximum TG value")):
    query = f"SELECT COUNT(T1.ID) FROM Examination AS T1 INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID WHERE T2.TG < ? AND T1.Symptoms IS NOT NULL"
    cursor.execute(query, (tg_max,))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get diagnosis from examination with specific TG range
@app.get("/v1/bird/thrombosis_prediction/examination_diagnosis_by_tg", summary="Get diagnosis from examination with specific TG range")
async def get_examination_diagnosis_by_tg(tg_max: float = Query(..., description="Maximum TG value")):
    query = f"SELECT T1.Diagnosis FROM Examination AS T1 INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID WHERE T2.TG < ? ORDER BY T2.TG DESC LIMIT 1"
    cursor.execute(query, (tg_max,))
    result = cursor.fetchone()
    return {"diagnosis": result[0]}

# Endpoint to get distinct IDs from laboratory with specific thrombosis and CPK range
@app.get("/v1/bird/thrombosis_prediction/laboratory_distinct_ids_by_thrombosis_cpk", summary="Get distinct IDs from laboratory with specific thrombosis and CPK range")
async def get_laboratory_distinct_ids_by_thrombosis_cpk(thrombosis: int = Query(..., description="Thrombosis value"), cpk_max: float = Query(..., description="Maximum CPK value")):
    query = f"SELECT DISTINCT T1.ID FROM Laboratory AS T1 INNER JOIN Examination AS T2 ON T1.ID = T2.ID WHERE T2.Thrombosis = ? AND T1.CPK < ?"
    cursor.execute(query, (thrombosis, cpk_max))
    result = cursor.fetchall()
    return {"IDs": [row[0] for row in result]}

# Endpoint to get count of patients with specific CPK range and positive KCT, RVVT, or LAC
@app.get("/v1/bird/thrombosis_prediction/patient_count_by_cpk_positive_tests", summary="Get count of patients with specific CPK range and positive KCT, RVVT, or LAC")
async def get_patient_count_by_cpk_positive_tests(cpk_max: float = Query(..., description="Maximum CPK value")):
    query = f"SELECT COUNT(T1.ID) FROM Patient AS T1 INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID INNER JOIN Examination AS T3 ON T1.ID = T3.ID WHERE T2.CPK < ? AND (T3.KCT = '+' OR T3.RVVT = '+' OR T3.LAC = '+')"
    cursor.execute(query, (cpk_max,))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get birthday from patient with specific GLU range
@app.get("/v1/bird/thrombosis_prediction/patient_birthday_by_glu", summary="Get birthday from patient with specific GLU range")
async def get_patient_birthday_by_glu(glu_min: float = Query(..., description="Minimum GLU value")):
    query = f"SELECT T1.Birthday FROM Patient AS T1 INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID WHERE T2.GLU > ? ORDER BY T1.Birthday ASC LIMIT 1"
    cursor.execute(query, (glu_min,))
    result = cursor.fetchone()
    return {"birthday": result[0]}

# Endpoint to get count of patients with specific GLU range and thrombosis value
@app.get("/v1/bird/thrombosis_prediction/patient_count_by_glu_thrombosis", summary="Get count of patients with specific GLU range and thrombosis value")
async def get_patient_count_by_glu_thrombosis(glu_max: float = Query(..., description="Maximum GLU value"), thrombosis: int = Query(..., description="Thrombosis value")):
    query = f"SELECT COUNT(T1.ID) FROM Patient AS T1 INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID INNER JOIN Examination AS T3 ON T1.ID = T3.ID WHERE T2.GLU < ? AND T3.Thrombosis = ?"
    cursor.execute(query, (glu_max, thrombosis))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get count of patients with specific WBC range and admission value
@app.get("/v1/bird/thrombosis_prediction/patient_count_by_wbc_admission", summary="Get count of patients with specific WBC range and admission value")
async def get_patient_count_by_wbc_admission(wbc_min: float = Query(..., description="Minimum WBC value"), wbc_max: float = Query(..., description="Maximum WBC value"), admission: str = Query(..., description="Admission value")):
    query = f"SELECT COUNT(T1.ID) FROM Patient AS T1 INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID WHERE T2.WBC BETWEEN ? AND ? AND T1.Admission = ?"
    cursor.execute(query, (wbc_min, wbc_max, admission))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get count of patients with specific diagnosis and WBC range
@app.get("/v1/bird/thrombosis_prediction/patient_count_by_diagnosis_wbc", summary="Get count of patients with specific diagnosis and WBC range")
async def get_patient_count_by_diagnosis_wbc(diagnosis: str = Query(..., description="Diagnosis of the patient"), wbc_min: float = Query(..., description="Minimum WBC value"), wbc_max: float = Query(..., description="Maximum WBC value")):
    query = f"SELECT COUNT(T1.ID) FROM Patient AS T1 INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID WHERE T1.Diagnosis = ? AND T2.WBC BETWEEN ? AND ?"
    cursor.execute(query, (diagnosis, wbc_min, wbc_max))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get distinct IDs from patient with specific RBC range and admission value
@app.get("/v1/bird/thrombosis_prediction/patient_distinct_ids_by_rbc_admission", summary="Get distinct IDs from patient with specific RBC range and admission value")
async def get_patient_distinct_ids_by_rbc_admission(rbc_min: float = Query(..., description="Minimum RBC value"), rbc_max: float = Query(..., description="Maximum RBC value"), admission: str = Query(..., description="Admission value")):
    query = f"SELECT DISTINCT T1.ID FROM Patient AS T1 INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID WHERE (T2.RBC <= ? OR T2.RBC >= ?) AND T1.Admission = ?"
    cursor.execute(query, (rbc_min, rbc_max, admission))
    result = cursor.fetchall()
    return {"IDs": [row[0] for row in result]}

# Endpoint to get count of patients with specific PLT range and non-null diagnosis
@app.get("/v1/bird/thrombosis_prediction/patient_count_by_plt_diagnosis", summary="Get count of patients with specific PLT range and non-null diagnosis")
async def get_patient_count_by_plt_diagnosis(plt_min: float = Query(..., description="Minimum PLT value"), plt_max: float = Query(..., description="Maximum PLT value")):
    query = f"SELECT COUNT(T1.ID) FROM Patient AS T1 INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID WHERE T2.PLT > ? AND T2.PLT < ? AND T1.Diagnosis IS NOT NULL"
    cursor.execute(query, (plt_min, plt_max))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get PLT values for a given diagnosis and PLT range
@app.get("/v1/bird/thrombosis_prediction/plt_values", summary="Get PLT values for a given diagnosis and PLT range")
async def get_plt_values(diagnosis: str = Query(..., description="Diagnosis of the patient"),
                         plt_min: int = Query(..., description="Minimum PLT value"),
                         plt_max: int = Query(..., description="Maximum PLT value")):
    query = """
    SELECT T2.PLT
    FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    WHERE T1.Diagnosis = ? AND T2.PLT BETWEEN ? AND ?
    """
    cursor.execute(query, (diagnosis, plt_min, plt_max))
    results = cursor.fetchall()
    return {"plt_values": results}

# Endpoint to get average PT for a given PT threshold and sex
@app.get("/v1/bird/thrombosis_prediction/avg_pt", summary="Get average PT for a given PT threshold and sex")
async def get_avg_pt(pt_threshold: int = Query(..., description="PT threshold value"),
                     sex: str = Query(..., description="Sex of the patient")):
    query = """
    SELECT AVG(T2.PT)
    FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    WHERE T2.PT < ? AND T1.SEX = ?
    """
    cursor.execute(query, (pt_threshold, sex))
    result = cursor.fetchone()
    return {"avg_pt": result[0]}

# Endpoint to get count of patients for given PT and Thrombosis ranges
@app.get("/v1/bird/thrombosis_prediction/patient_count", summary="Get count of patients for given PT and Thrombosis ranges")
async def get_patient_count(pt_threshold: int = Query(..., description="PT threshold value"),
                            thrombosis_min: int = Query(..., description="Minimum Thrombosis value"),
                            thrombosis_max: int = Query(..., description="Maximum Thrombosis value")):
    query = """
    SELECT COUNT(T1.ID)
    FROM Patient AS T1
    INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID
    INNER JOIN Examination AS T3 ON T1.ID = T3.ID
    WHERE T2.PT < ? AND T3.Thrombosis BETWEEN ? AND ?
    """
    cursor.execute(query, (pt_threshold, thrombosis_min, thrombosis_max))
    result = cursor.fetchone()
    return {"patient_count": result[0]}
