from fastapi import APIRouter, Query
import sqlite3

app = APIRouter()

# Database connection
conn = sqlite3.connect('invocable_api_hub/db/bird/test/california_schools.sqlite', check_same_thread=False)
cursor = conn.cursor()

# Endpoint to get the free meal count ratio for a given county
@app.get("/v1/bird/california_schools/free_meal_count_ratio", summary="Get free meal count ratio for a given county")
async def get_free_meal_count_ratio(county_name: str = Query(..., description="Name of the county")):
    query = """
    SELECT `Free Meal Count (K-12)` / `Enrollment (K-12)`
    FROM frpm
    WHERE `County Name` = ?
    ORDER BY (CAST(`Free Meal Count (K-12)` AS REAL) / `Enrollment (K-12)`) DESC
    LIMIT 1
    """
    cursor.execute(query, (county_name,))  # Execute the query
    result = cursor.fetchone()  # Get the result of running the SQL query
    return {"free_meal_count_ratio": result}

# Endpoint to get free meal count ratio for a given educational option type
@app.get("/v1/bird/california_schools/free_meal_count_ratio_educational_option", summary="Get free meal count ratio for a given educational option type")
async def get_free_meal_count_ratio_educational_option(educational_option_type: str = Query(..., description="Educational option type")):
    query = """
    SELECT `Free Meal Count (Ages 5-17)` / `Enrollment (Ages 5-17)`
    FROM frpm
    WHERE `Educational Option Type` = ? AND `Free Meal Count (Ages 5-17)` / `Enrollment (Ages 5-17)` IS NOT NULL
    ORDER BY `Free Meal Count (Ages 5-17)` / `Enrollment (Ages 5-17)` ASC
    LIMIT 3
    """
    cursor.execute(query, (educational_option_type,))
    result = cursor.fetchall()
    return {"free_meal_count_ratio": result}

# Endpoint to get zip codes for a given district name and charter school status
@app.get("/v1/bird/california_schools/zip_codes", summary="Get zip codes for a given district name and charter school status")
async def get_zip_codes(district_name: str = Query(..., description="Name of the district"), charter_school: int = Query(..., description="Charter school status (1 for yes, 0 for no)")):
    query = """
    SELECT T2.Zip
    FROM frpm AS T1
    INNER JOIN schools AS T2 ON T1.CDSCode = T2.CDSCode
    WHERE T1.`District Name` = ? AND T1.`Charter School (Y/N)` = ?
    """
    cursor.execute(query, (district_name, charter_school))
    result = cursor.fetchall()
    return {"zip_codes": result}

# Endpoint to get mail street for the highest FRPM count (K-12)
@app.get("/v1/bird/california_schools/mail_street_highest_frpm", summary="Get mail street for the highest FRPM count (K-12)")
async def get_mail_street_highest_frpm():
    query = """
    SELECT T2.MailStreet
    FROM frpm AS T1
    INNER JOIN schools AS T2 ON T1.CDSCode = T2.CDSCode
    ORDER BY T1.`FRPM Count (K-12)` DESC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"mail_street": result}

# Endpoint to get phone numbers for charter schools with specific funding type and open date
@app.get("/v1/bird/california_schools/phone_numbers_charter_schools", summary="Get phone numbers for charter schools with specific funding type and open date")
async def get_phone_numbers_charter_schools(charter_funding_type: str = Query(..., description="Charter funding type"), open_date: str = Query(..., description="Open date (YYYY-MM-DD)")):
    query = """
    SELECT T2.Phone
    FROM frpm AS T1
    INNER JOIN schools AS T2 ON T1.CDSCode = T2.CDSCode
    WHERE T1.`Charter Funding Type` = ? AND T1.`Charter School (Y/N)` = 1 AND T2.OpenDate > ?
    """
    cursor.execute(query, (charter_funding_type, open_date))
    result = cursor.fetchall()
    return {"phone_numbers": result}

# Endpoint to get count of distinct schools with specific virtual status and average math score
@app.get("/v1/bird/california_schools/count_distinct_schools", summary="Get count of distinct schools with specific virtual status and average math score")
async def get_count_distinct_schools(virtual_status: str = Query(..., description="Virtual status (F for false, T for true)"), avg_scr_math: int = Query(..., description="Average math score")):
    query = """
    SELECT COUNT(DISTINCT T2.School)
    FROM satscores AS T1
    INNER JOIN schools AS T2 ON T1.cds = T2.CDSCode
    WHERE T2.Virtual = ? AND T1.AvgScrMath > ?
    """
    cursor.execute(query, (virtual_status, avg_scr_math))
    result = cursor.fetchone()
    return {"count_distinct_schools": result}

# Endpoint to get schools with specific magnet status and number of test takers
@app.get("/v1/bird/california_schools/schools_magnet_status", summary="Get schools with specific magnet status and number of test takers")
async def get_schools_magnet_status(magnet_status: int = Query(..., description="Magnet status (1 for yes, 0 for no)"), num_test_takers: int = Query(..., description="Number of test takers")):
    query = """
    SELECT T2.School
    FROM satscores AS T1
    INNER JOIN schools AS T2 ON T1.cds = T2.CDSCode
    WHERE T2.Magnet = ? AND T1.NumTstTakr > ?
    """
    cursor.execute(query, (magnet_status, num_test_takers))
    result = cursor.fetchall()
    return {"schools": result}

# Endpoint to get phone number for the highest number of students scoring 1500 or above
@app.get("/v1/bird/california_schools/phone_number_highest_ge1500", summary="Get phone number for the highest number of students scoring 1500 or above")
async def get_phone_number_highest_ge1500():
    query = """
    SELECT T2.Phone
    FROM satscores AS T1
    INNER JOIN schools AS T2 ON T1.cds = T2.CDSCode
    ORDER BY T1.NumGE1500 DESC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"phone_number": result}

# Endpoint to get number of test takers for the school with the highest FRPM count (K-12)
@app.get("/v1/bird/california_schools/num_test_takers_highest_frpm", summary="Get number of test takers for the school with the highest FRPM count (K-12)")
async def get_num_test_takers_highest_frpm():
    query = """
    SELECT NumTstTakr
    FROM satscores
    WHERE cds = (
        SELECT CDSCode
        FROM frpm
        ORDER BY `FRPM Count (K-12)` DESC
        LIMIT 1
    )
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"num_test_takers": result}

# Endpoint to get count of school codes with specific average math score and charter funding type
@app.get("/v1/bird/california_schools/count_school_codes", summary="Get count of school codes with specific average math score and charter funding type")
async def get_count_school_codes(avg_scr_math: int = Query(..., description="Average math score"), charter_funding_type: str = Query(..., description="Charter funding type")):
    query = """
    SELECT COUNT(T2.`School Code`)
    FROM satscores AS T1
    INNER JOIN frpm AS T2 ON T1.cds = T2.CDSCode
    WHERE T1.AvgScrMath > ? AND T2.`Charter Funding Type` = ?
    """
    cursor.execute(query, (avg_scr_math, charter_funding_type))
    result = cursor.fetchone()
    return {"count_school_codes": result}

# Endpoint to get FRPM count (Ages 5-17) for the highest average reading score
@app.get("/v1/bird/california_schools/frpm_count_highest_avg_read", summary="Get FRPM count (Ages 5-17) for the highest average reading score")
async def get_frpm_count_highest_avg_read():
    query = """
    SELECT T2.`FRPM Count (Ages 5-17)`
    FROM satscores AS T1
    INNER JOIN frpm AS T2 ON T1.cds = T2.CDSCode
    ORDER BY T1.AvgScrRead DESC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"frpm_count": result}

# Endpoint to get CDS codes for schools with specific enrollment criteria
@app.get("/v1/bird/california_schools/cds_codes_enrollment_criteria", summary="Get CDS codes for schools with specific enrollment criteria")
async def get_cds_codes_enrollment_criteria(enrollment_k12: int = Query(..., description="Enrollment (K-12)"), enrollment_ages_5_17: int = Query(..., description="Enrollment (Ages 5-17)")):
    query = """
    SELECT T2.CDSCode
    FROM schools AS T1
    INNER JOIN frpm AS T2 ON T1.CDSCode = T2.CDSCode
    WHERE T2.`Enrollment (K-12)` + T2.`Enrollment (Ages 5-17)` > ?
    """
    cursor.execute(query, (enrollment_k12 + enrollment_ages_5_17,))
    result = cursor.fetchall()
    return {"cds_codes": result}

# Endpoint to get maximum free meal count ratio for schools with specific GE1500 ratio
@app.get("/v1/bird/california_schools/max_free_meal_count_ratio", summary="Get maximum free meal count ratio for schools with specific GE1500 ratio")
async def get_max_free_meal_count_ratio(ge1500_ratio: float = Query(..., description="GE1500 ratio")):
    query = """
    SELECT MAX(CAST(T1.`Free Meal Count (Ages 5-17)` AS REAL) / T1.`Enrollment (Ages 5-17)`)
    FROM frpm AS T1
    INNER JOIN satscores AS T2 ON T1.CDSCode = T2.cds
    WHERE CAST(T2.NumGE1500 AS REAL) / T2.NumTstTakr > ?
    """
    cursor.execute(query, (ge1500_ratio,))
    result = cursor.fetchone()
    return {"max_free_meal_count_ratio": result}

# Endpoint to get phone numbers for schools with specific GE1500 ratio
@app.get("/v1/bird/california_schools/phone_numbers_ge1500_ratio", summary="Get phone numbers for schools with specific GE1500 ratio")
async def get_phone_numbers_ge1500_ratio(limit: int = Query(..., description="Limit of results")):
    query = """
    SELECT T1.Phone
    FROM schools AS T1
    INNER JOIN satscores AS T2 ON T1.CDSCode = T2.cds
    ORDER BY CAST(T2.NumGE1500 AS REAL) / T2.NumTstTakr DESC
    LIMIT ?
    """
    cursor.execute(query, (limit,))
    result = cursor.fetchall()
    return {"phone_numbers": result}

# Endpoint to get NCES school codes for schools with specific enrollment criteria
@app.get("/v1/bird/california_schools/nces_school_codes_enrollment_criteria", summary="Get NCES school codes for schools with specific enrollment criteria")
async def get_nces_school_codes_enrollment_criteria(limit: int = Query(..., description="Limit of results")):
    query = """
    SELECT T1.NCESSchool
    FROM schools AS T1
    INNER JOIN frpm AS T2 ON T1.CDSCode = T2.CDSCode
    ORDER BY T2.`Enrollment (Ages 5-17)` DESC
    LIMIT ?
    """
    cursor.execute(query, (limit,))
    result = cursor.fetchall()
    return {"nces_school_codes": result}

# Endpoint to get district for schools with specific status type and average reading score
@app.get("/v1/bird/california_schools/district_status_avg_read", summary="Get district for schools with specific status type and average reading score")
async def get_district_status_avg_read(status_type: str = Query(..., description="Status type"), limit: int = Query(..., description="Limit of results")):
    query = """
    SELECT T1.District
    FROM schools AS T1
    INNER JOIN satscores AS T2 ON T1.CDSCode = T2.cds
    WHERE T1.StatusType = ?
    ORDER BY T2.AvgScrRead DESC
    LIMIT ?
    """
    cursor.execute(query, (status_type, limit))
    result = cursor.fetchall()
    return {"district": result}

# Endpoint to get count of CDS codes for schools with specific status type, number of test takers, and county
@app.get("/v1/bird/california_schools/count_cds_codes_status_test_takers_county", summary="Get count of CDS codes for schools with specific status type, number of test takers, and county")
async def get_count_cds_codes_status_test_takers_county(status_type: str = Query(..., description="Status type"), num_test_takers: int = Query(..., description="Number of test takers"), county: str = Query(..., description="County")):
    query = """
    SELECT COUNT(T1.CDSCode)
    FROM schools AS T1
    INNER JOIN satscores AS T2 ON T1.CDSCode = T2.cds
    WHERE T1.StatusType = ? AND T2.NumTstTakr < ? AND T1.County = ?
    """
    cursor.execute(query, (status_type, num_test_takers, county))
    result = cursor.fetchone()
    return {"count_cds_codes": result}

# Endpoint to get charter numbers, average writing scores, and writing score ranks for schools with specific average writing score
@app.get("/v1/bird/california_schools/charter_numbers_avg_writing_scores", summary="Get charter numbers, average writing scores, and writing score ranks for schools with specific average writing score")
async def get_charter_numbers_avg_writing_scores(avg_scr_write: int = Query(..., description="Average writing score")):
    query = """
    SELECT CharterNum, AvgScrWrite, RANK() OVER (ORDER BY AvgScrWrite DESC) AS WritingScoreRank
    FROM schools AS T1
    INNER JOIN satscores AS T2 ON T1.CDSCode = T2.cds
    WHERE T2.AvgScrWrite > ? AND CharterNum is not null
    """
    cursor.execute(query, (avg_scr_write,))
    result = cursor.fetchall()
    return {"charter_numbers_avg_writing_scores": result}

# Endpoint to get count of CDS codes for schools with specific charter funding type, county, and number of test takers
@app.get("/v1/bird/california_schools/count_cds_codes_charter_funding_county_test_takers", summary="Get count of CDS codes for schools with specific charter funding type, county, and number of test takers")
async def get_count_cds_codes_charter_funding_county_test_takers(charter_funding_type: str = Query(..., description="Charter funding type"), county: str = Query(..., description="County"), num_test_takers: int = Query(..., description="Number of test takers")):
    query = """
    SELECT COUNT(T1.CDSCode)
    FROM frpm AS T1
    INNER JOIN satscores AS T2 ON T1.CDSCode = T2.cds
    WHERE T1.`Charter Funding Type` = ? AND T1.`County Name` = ? AND T2.NumTstTakr <= ?
    """
    cursor.execute(query, (charter_funding_type, county, num_test_takers))
    result = cursor.fetchone()
    return {"count_cds_codes": result}

# Endpoint to get phone number for the highest average math score
@app.get("/v1/bird/california_schools/phone_number_highest_avg_math", summary="Get phone number for the highest average math score")
async def get_phone_number_highest_avg_math():
    query = """
    SELECT T1.Phone
    FROM schools AS T1
    INNER JOIN satscores AS T2 ON T1.CDSCode = T2.cds
    ORDER BY T2.AvgScrMath DESC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"phone_number": result}

# Endpoint to get count of school names for schools with specific county, low grade, and high grade
@app.get("/v1/bird/california_schools/count_school_names_county_grades", summary="Get count of school names for schools with specific county, low grade, and high grade")
async def get_count_school_names_county_grades(county: str = Query(..., description="County"), low_grade: int = Query(..., description="Low grade"), high_grade: int = Query(..., description="High grade")):
    query = """
    SELECT COUNT(T1.`School Name`)
    FROM frpm AS T1
    INNER JOIN schools AS T2 ON T1.CDSCode = T2.CDSCode
    WHERE T2.County = ? AND T1.`Low Grade` = ? AND T1.`High Grade` = ?
    """
    cursor.execute(query, (county, low_grade, high_grade))
    result = cursor.fetchone()
    return {"count_school_names": result}

# Endpoint to get count of CDS codes for schools with specific county, free meal count, and FRPM count
@app.get("/v1/bird/california_schools/count_cds_codes_county_free_meal_frpm", summary="Get count of CDS codes for schools with specific county, free meal count, and FRPM count")
async def get_count_cds_codes_county_free_meal_frpm(county_name: str = Query(..., description="County name"), free_meal_count: int = Query(..., description="Free meal count"), frpm_count: int = Query(..., description="FRPM count")):
    query = """
    SELECT COUNT(CDSCode)
    FROM frpm
    WHERE `County Name` = ? AND `Free Meal Count (K-12)` > ? AND `FRPM Count (K-12)` < ?
    """
    cursor.execute(query, (county_name, free_meal_count, frpm_count))
    result = cursor.fetchone()
    return {"count_cds_codes": result}

# Endpoint to get school name for a specific county and non-null school name
@app.get("/v1/bird/california_schools/school_name_county_non_null", summary="Get school name for a specific county and non-null school name")
async def get_school_name_county_non_null(county_name: str = Query(..., description="County name")):
    query = """
    SELECT sname
    FROM satscores
    WHERE cname = ? AND sname IS NOT NULL
    ORDER BY NumTstTakr DESC
    LIMIT 1
    """
    cursor.execute(query, (county_name,))
    result = cursor.fetchone()
    return {"school_name": result}

# Endpoint to get school and street for schools with specific enrollment criteria
@app.get("/v1/bird/california_schools/school_street_enrollment_criteria", summary="Get school and street for schools with specific enrollment criteria")
async def get_school_street_enrollment_criteria(enrollment_diff: int = Query(..., description="Enrollment difference")):
    query = """
    SELECT T1.School, T1.Street
    FROM schools AS T1
    INNER JOIN frpm AS T2 ON T1.CDSCode = T2.CDSCode
    WHERE T2.`Enrollment (K-12)` - T2.`Enrollment (Ages 5-17)` > ?
    """
    cursor.execute(query, (enrollment_diff,))
    result = cursor.fetchall()
    return {"school_street": result}

# Endpoint to get school names for schools with specific free meal count ratio and number of students scoring 1500 or above
@app.get("/v1/bird/california_schools/school_names_free_meal_ge1500", summary="Get school names for schools with specific free meal count ratio and number of students scoring 1500 or above")
async def get_school_names_free_meal_ge1500(free_meal_ratio: float = Query(..., description="Free meal count ratio"), num_ge1500: int = Query(..., description="Number of students scoring 1500 or above")):
    query = """
    SELECT T2.`School Name`
    FROM satscores AS T1
    INNER JOIN frpm AS T2 ON T1.cds = T2.CDSCode
    WHERE CAST(T2.`Free Meal Count (K-12)` AS REAL) / T2.`Enrollment (K-12)` > ? AND T1.NumGE1500 > ?
    """
    cursor.execute(query, (free_meal_ratio, num_ge1500))
    result = cursor.fetchall()
    return {"school_names": result}

# Endpoint to get school names and charter funding types for schools with specific district name and average math score
@app.get("/v1/bird/california_schools/school_names_charter_funding_avg_math", summary="Get school names and charter funding types for schools with specific district name and average math score")
async def get_school_names_charter_funding_avg_math(district_name: str = Query(..., description="District name"), avg_scr_math: int = Query(..., description="Average math score")):
    query = """
    SELECT T1.sname, T2.`Charter Funding Type`
    FROM satscores AS T1
    INNER JOIN frpm AS T2 ON T1.cds = T2.CDSCode
    WHERE T2.`District Name` LIKE ?
    GROUP BY T1.sname, T2.`Charter Funding Type`
    HAVING CAST(SUM(T1.AvgScrMath) AS REAL) / COUNT(T1.cds) > ?
    """
    cursor.execute(query, (district_name + '%', avg_scr_math))
    result = cursor.fetchall()
    return {"school_names_charter_funding": result}

# Endpoint to get school information for schools with specific county, free meal count, and school type
@app.get("/v1/bird/california_schools/school_info_county_free_meal_school_type", summary="Get school information for schools with specific county, free meal count, and school type")
async def get_school_info_county_free_meal_school_type(county: str = Query(..., description="County"), free_meal_count: int = Query(..., description="Free meal count"), school_type: str = Query(..., description="School type")):
    query = """
    SELECT T1.`School Name`, T2.Street, T2.City, T2.State, T2.Zip
    FROM frpm AS T1
    INNER JOIN schools AS T2 ON T1.CDSCode = T2.CDSCode
    WHERE T2.County = ? AND T1.`Free Meal Count (Ages 5-17)` > ? AND T1.`School Type` = ?
    """
    cursor.execute(query, (county, free_meal_count, school_type))
    result = cursor.fetchall()
    return {"school_info": result}

# Endpoint to get school, average writing score, and phone for schools with specific open or closed date criteria
@app.get("/v1/bird/california_schools/school_avg_writing_score_phone_date_criteria", summary="Get school, average writing score, and phone for schools with specific open or closed date criteria")
async def get_school_avg_writing_score_phone_date_criteria(open_date: str = Query(..., description="Open date (YYYY)"), closed_date: str = Query(..., description="Closed date (YYYY)")):
    query = """
    SELECT T2.School, T1.AvgScrWrite, T2.Phone
    FROM schools AS T2
    LEFT JOIN satscores AS T1 ON T2.CDSCode = T1.cds
    WHERE strftime('%Y', T2.OpenDate) > ? OR strftime('%Y', T2.ClosedDate) < ?
    """
    cursor.execute(query, (open_date, closed_date))
    result = cursor.fetchall()
    return {"school_avg_writing_score_phone": result}

# Endpoint to get school and DOC for schools with specific funding type and enrollment criteria
@app.get("/v1/bird/california_schools/school_doc_funding_enrollment_criteria", summary="Get school and DOC for schools with specific funding type and enrollment criteria")
async def get_school_doc_funding_enrollment_criteria(funding_type: str = Query(..., description="Funding type")):
    query = """
    SELECT T2.School, T2.DOC
    FROM frpm AS T1
    INNER JOIN schools AS T2 ON T1.CDSCode = T2.CDSCode
    WHERE T2.FundingType = ? AND (T1.`Enrollment (K-12)` - T1.`Enrollment (Ages 5-17)`) > (
        SELECT AVG(T3.`Enrollment (K-12)` - T3.`Enrollment (Ages 5-17)`)
        FROM frpm AS T3
        INNER JOIN schools AS T4 ON T3.CDSCode = T4.CDSCode
        WHERE T4.FundingType = ?
    )
    """
    cursor.execute(query, (funding_type, funding_type))
    result = cursor.fetchall()
    return {"school_doc": result}

# Endpoint to get open date for the school with the highest enrollment (K-12)
@app.get("/v1/bird/california_schools/open_date_highest_enrollment", summary="Get open date for the school with the highest enrollment (K-12)")
async def get_open_date_highest_enrollment():
    query = """
    SELECT T2.OpenDate
    FROM frpm AS T1
    INNER JOIN schools AS T2 ON T1.CDSCode = T2.CDSCode
    ORDER BY T1.`Enrollment (K-12)` DESC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"open_date": result}

# Endpoint to get cities with the lowest enrollment
@app.get("/v1/bird/california_schools/cities_lowest_enrollment", summary="Get cities with the lowest enrollment")
async def get_cities_lowest_enrollment(limit: int = Query(5, description="Limit the number of results")):
    query = """
    SELECT T2.City FROM frpm AS T1
    INNER JOIN schools AS T2 ON T1.CDSCode = T2.CDSCode
    GROUP BY T2.City
    ORDER BY SUM(T1.`Enrollment (K-12)`) ASC
    LIMIT ?
    """
    cursor.execute(query, (limit,))
    return cursor.fetchall()

# Endpoint to get free meal ratio
@app.get("/v1/bird/california_schools/free_meal_ratio", summary="Get free meal ratio")
async def get_free_meal_ratio(limit: int = Query(9, description="Limit the number of results"), offset: int = Query(2, description="Offset the results")):
    query = """
    SELECT CAST(`Free Meal Count (K-12)` AS REAL) / `Enrollment (K-12)` FROM frpm
    ORDER BY `Enrollment (K-12)` DESC
    LIMIT ? OFFSET ?
    """
    cursor.execute(query, (limit, offset))
    return cursor.fetchall()

# Endpoint to get FRPM ratio for specific SOC
@app.get("/v1/bird/california_schools/frpm_ratio_by_soc", summary="Get FRPM ratio for specific SOC")
async def get_frpm_ratio_by_soc(soc: int = Query(66, description="SOC value"), limit: int = Query(5, description="Limit the number of results")):
    query = """
    SELECT CAST(T1.`FRPM Count (K-12)` AS REAL) / T1.`Enrollment (K-12)` FROM frpm AS T1
    INNER JOIN schools AS T2 ON T1.CDSCode = T2.CDSCode
    WHERE T2.SOC = ?
    ORDER BY T1.`FRPM Count (K-12)` DESC
    LIMIT ?
    """
    cursor.execute(query, (soc, limit))
    return cursor.fetchall()

# Endpoint to get school websites with specific free meal count
@app.get("/v1/bird/california_schools/school_websites_by_free_meal_count", summary="Get school websites with specific free meal count")
async def get_school_websites_by_free_meal_count(min_count: int = Query(1900, description="Minimum free meal count"), max_count: int = Query(2000, description="Maximum free meal count")):
    query = """
    SELECT T2.Website, T1.`School Name` FROM frpm AS T1
    INNER JOIN schools AS T2 ON T1.CDSCode = T2.CDSCode
    WHERE T1.`Free Meal Count (Ages 5-17)` BETWEEN ? AND ? AND T2.Website IS NOT NULL
    """
    cursor.execute(query, (min_count, max_count))
    return cursor.fetchall()

# Endpoint to get free meal ratio for specific admin
@app.get("/v1/bird/california_schools/free_meal_ratio_by_admin", summary="Get free meal ratio for specific admin")
async def get_free_meal_ratio_by_admin(adm_fname: str = Query(..., description="Admin first name"), adm_lname: str = Query(..., description="Admin last name")):
    query = """
    SELECT CAST(T2.`Free Meal Count (Ages 5-17)` AS REAL) / T2.`Enrollment (Ages 5-17)` FROM schools AS T1
    INNER JOIN frpm AS T2 ON T1.CDSCode = T2.CDSCode
    WHERE T1.AdmFName1 = ? AND T1.AdmLName1 = ?
    """
    cursor.execute(query, (adm_fname, adm_lname))
    return cursor.fetchall()

# Endpoint to get admin emails for charter schools
@app.get("/v1/bird/california_schools/admin_emails_for_charter_schools", summary="Get admin emails for charter schools")
async def get_admin_emails_for_charter_schools(limit: int = Query(1, description="Limit the number of results")):
    query = """
    SELECT T2.AdmEmail1 FROM frpm AS T1
    INNER JOIN schools AS T2 ON T1.CDSCode = T2.CDSCode
    WHERE T1.`Charter School (Y/N)` = 1
    ORDER BY T1.`Enrollment (K-12)` ASC
    LIMIT ?
    """
    cursor.execute(query, (limit,))
    return cursor.fetchall()

# Endpoint to get admin names for top SAT scores
@app.get("/v1/bird/california_schools/admin_names_for_top_sat_scores", summary="Get admin names for top SAT scores")
async def get_admin_names_for_top_sat_scores(limit: int = Query(1, description="Limit the number of results")):
    query = """
    SELECT T2.AdmFName1, T2.AdmLName1, T2.AdmFName2, T2.AdmLName2, T2.AdmFName3, T2.AdmLName3 FROM satscores AS T1
    INNER JOIN schools AS T2 ON T1.cds = T2.CDSCode
    ORDER BY T1.NumGE1500 DESC
    LIMIT ?
    """
    cursor.execute(query, (limit,))
    return cursor.fetchall()

# Endpoint to get school addresses for top SAT scores
@app.get("/v1/bird/california_schools/school_addresses_for_top_sat_scores", summary="Get school addresses for top SAT scores")
async def get_school_addresses_for_top_sat_scores(limit: int = Query(1, description="Limit the number of results")):
    query = """
    SELECT T2.Street, T2.City, T2.State, T2.Zip FROM satscores AS T1
    INNER JOIN schools AS T2 ON T1.cds = T2.CDSCode
    ORDER BY CAST(T1.NumGE1500 AS REAL) / T1.NumTstTakr ASC
    LIMIT ?
    """
    cursor.execute(query, (limit,))
    return cursor.fetchall()

# Endpoint to get school websites for specific test takers range and county
@app.get("/v1/bird/california_schools/school_websites_by_test_takers_and_county", summary="Get school websites for specific test takers range and county")
async def get_school_websites_by_test_takers_and_county(min_takers: int = Query(2000, description="Minimum test takers"), max_takers: int = Query(3000, description="Maximum test takers"), county: str = Query(..., description="County name")):
    query = """
    SELECT T2.Website FROM satscores AS T1
    INNER JOIN schools AS T2 ON T1.cds = T2.CDSCode
    WHERE T1.NumTstTakr BETWEEN ? AND ? AND T2.County = ?
    """
    cursor.execute(query, (min_takers, max_takers, county))
    return cursor.fetchall()

# Endpoint to get average test takers for specific year and county
@app.get("/v1/bird/california_schools/average_test_takers_by_year_and_county", summary="Get average test takers for specific year and county")
async def get_average_test_takers_by_year_and_county(year: int = Query(..., description="Year"), county: str = Query(..., description="County name")):
    query = """
    SELECT AVG(T1.NumTstTakr) FROM satscores AS T1
    INNER JOIN schools AS T2 ON T1.cds = T2.CDSCode
    WHERE strftime('%Y', T2.OpenDate) = ? AND T2.County = ?
    """
    cursor.execute(query, (year, county))
    return cursor.fetchall()

# Endpoint to get school phones for specific district and non-null average score
@app.get("/v1/bird/california_schools/school_phones_by_district_and_avg_score", summary="Get school phones for specific district and non-null average score")
async def get_school_phones_by_district_and_avg_score(district: str = Query(..., description="District name"), limit: int = Query(1, description="Limit the number of results")):
    query = """
    SELECT T2.Phone FROM satscores AS T1
    INNER JOIN schools AS T2 ON T1.cds = T2.CDSCode
    WHERE T2.District = ? AND T1.AvgScrRead IS NOT NULL
    ORDER BY T1.AvgScrRead ASC
    LIMIT ?
    """
    cursor.execute(query, (district, limit))
    return cursor.fetchall()

# Endpoint to get top schools by average reading score and county
@app.get("/v1/bird/california_schools/top_schools_by_avg_reading_score_and_county", summary="Get top schools by average reading score and county")
async def get_top_schools_by_avg_reading_score_and_county(limit: int = Query(5, description="Limit the number of results")):
    query = """
    SELECT School FROM (
        SELECT T2.School, T1.AvgScrRead, RANK() OVER (PARTITION BY T2.County ORDER BY T1.AvgScrRead DESC) AS rnk
        FROM satscores AS T1
        INNER JOIN schools AS T2 ON T1.cds = T2.CDSCode
        WHERE T2.Virtual = 'F'
    ) ranked_schools
    WHERE rnk <= ?
    """
    cursor.execute(query, (limit,))
    return cursor.fetchall()

# Endpoint to get education operation names for top math scores
@app.get("/v1/bird/california_schools/ed_ops_names_for_top_math_scores", summary="Get education operation names for top math scores")
async def get_ed_ops_names_for_top_math_scores(limit: int = Query(1, description="Limit the number of results")):
    query = """
    SELECT T2.EdOpsName FROM satscores AS T1
    INNER JOIN schools AS T2 ON T1.cds = T2.CDSCode
    ORDER BY T1.AvgScrMath DESC
    LIMIT ?
    """
    cursor.execute(query, (limit,))
    return cursor.fetchall()

# Endpoint to get average math scores and county for non-null math scores
@app.get("/v1/bird/california_schools/avg_math_scores_and_county_for_non_null_math_scores", summary="Get average math scores and county for non-null math scores")
async def get_avg_math_scores_and_county_for_non_null_math_scores(limit: int = Query(1, description="Limit the number of results")):
    query = """
    SELECT T1.AvgScrMath, T2.County FROM satscores AS T1
    INNER JOIN schools AS T2 ON T1.cds = T2.CDSCode
    WHERE T1.AvgScrMath IS NOT NULL
    ORDER BY T1.AvgScrMath + T1.AvgScrRead + T1.AvgScrWrite ASC
    LIMIT ?
    """
    cursor.execute(query, (limit,))
    return cursor.fetchall()

# Endpoint to get average writing scores and city for top GE1500 scores
@app.get("/v1/bird/california_schools/avg_writing_scores_and_city_for_top_ge1500_scores", summary="Get average writing scores and city for top GE1500 scores")
async def get_avg_writing_scores_and_city_for_top_ge1500_scores(limit: int = Query(1, description="Limit the number of results")):
    query = """
    SELECT T1.AvgScrWrite, T2.City FROM satscores AS T1
    INNER JOIN schools AS T2 ON T1.cds = T2.CDSCode
    ORDER BY T1.NumGE1500 DESC
    LIMIT ?
    """
    cursor.execute(query, (limit,))
    return cursor.fetchall()

# Endpoint to get school and average writing scores for specific admin
@app.get("/v1/bird/california_schools/school_and_avg_writing_scores_by_admin", summary="Get school and average writing scores for specific admin")
async def get_school_and_avg_writing_scores_by_admin(adm_fname: str = Query(..., description="Admin first name"), adm_lname: str = Query(..., description="Admin last name")):
    query = """
    SELECT T2.School, T1.AvgScrWrite FROM satscores AS T1
    INNER JOIN schools AS T2 ON T1.cds = T2.CDSCode
    WHERE T2.AdmFName1 = ? AND T2.AdmLName1 = ?
    """
    cursor.execute(query, (adm_fname, adm_lname))
    return cursor.fetchall()

# Endpoint to get school for specific DOC and enrollment order
@app.get("/v1/bird/california_schools/school_by_doc_and_enrollment_order", summary="Get school for specific DOC and enrollment order")
async def get_school_by_doc_and_enrollment_order(doc: int = Query(..., description="DOC value"), limit: int = Query(1, description="Limit the number of results")):
    query = """
    SELECT T2.School FROM frpm AS T1
    INNER JOIN schools AS T2 ON T1.CDSCode = T2.CDSCode
    WHERE T2.DOC = ?
    ORDER BY T1.`Enrollment (K-12)` DESC
    LIMIT ?
    """
    cursor.execute(query, (doc, limit))
    return cursor.fetchall()

# Endpoint to get school count ratio for specific DOC, county, and year
@app.get("/v1/bird/california_schools/school_count_ratio_by_doc_county_and_year", summary="Get school count ratio for specific DOC, county, and year")
async def get_school_count_ratio_by_doc_county_and_year(doc: int = Query(..., description="DOC value"), county: str = Query(..., description="County name"), year: int = Query(..., description="Year")):
    query = """
    SELECT CAST(COUNT(School) AS REAL) / 12 FROM schools
    WHERE DOC = ? AND County = ? AND strftime('%Y', OpenDate) = ?
    """
    cursor.execute(query, (doc, county, year))
    return cursor.fetchall()

# Endpoint to get school count ratio for specific DOCs and county
@app.get("/v1/bird/california_schools/school_count_ratio_by_docs_and_county", summary="Get school count ratio for specific DOCs and county")
async def get_school_count_ratio_by_docs_and_county(doc1: int = Query(..., description="First DOC value"), doc2: int = Query(..., description="Second DOC value"), county: str = Query(..., description="County name")):
    query = """
    SELECT CAST(SUM(CASE WHEN DOC = ? THEN 1 ELSE 0 END) AS REAL) / SUM(CASE WHEN DOC = ? THEN 1 ELSE 0 END) FROM schools
    WHERE StatusType = 'Merged' AND County = ?
    """
    cursor.execute(query, (doc1, doc2, county))
    return cursor.fetchall()

# Endpoint to get closed schools for county with the most closed schools
@app.get("/v1/bird/california_schools/closed_schools_for_county_with_most_closed_schools", summary="Get closed schools for county with the most closed schools")
async def get_closed_schools_for_county_with_most_closed_schools():
    query = """
    SELECT DISTINCT County, School, ClosedDate FROM schools
    WHERE County = (
        SELECT County FROM schools
        WHERE StatusType = 'Closed'
        GROUP BY County
        ORDER BY COUNT(School) DESC
        LIMIT 1
    ) AND StatusType = 'Closed' AND school IS NOT NULL
    """
    cursor.execute(query)
    return cursor.fetchall()

# Endpoint to get school mail street and name for top math scores
@app.get("/v1/bird/california_schools/school_mail_street_and_name_for_top_math_scores", summary="Get school mail street and name for top math scores")
async def get_school_mail_street_and_name_for_top_math_scores(limit: int = Query(6, description="Limit the number of results"), offset: int = Query(1, description="Offset the results")):
    query = """
    SELECT T2.MailStreet, T2.School FROM satscores AS T1
    INNER JOIN schools AS T2 ON T1.cds = T2.CDSCode
    ORDER BY T1.AvgScrMath DESC
    LIMIT ? OFFSET ?
    """
    cursor.execute(query, (limit, offset))
    return cursor.fetchall()

# Endpoint to get school mail street and name for non-null reading scores
@app.get("/v1/bird/california_schools/school_mail_street_and_name_for_non_null_reading_scores", summary="Get school mail street and name for non-null reading scores")
async def get_school_mail_street_and_name_for_non_null_reading_scores(limit: int = Query(1, description="Limit the number of results")):
    query = """
    SELECT T2.MailStreet, T2.School FROM satscores AS T1
    INNER JOIN schools AS T2 ON T1.cds = T2.CDSCode
    WHERE T1.AvgScrRead IS NOT NULL
    ORDER BY T1.AvgScrRead ASC
    LIMIT ?
    """
    cursor.execute(query, (limit,))
    return cursor.fetchall()

# Endpoint to get school count for specific mail city and total score
@app.get("/v1/bird/california_schools/school_count_for_mail_city_and_total_score", summary="Get school count for specific mail city and total score")
async def get_school_count_for_mail_city_and_total_score(mail_city: str = Query(..., description="Mail city"), min_score: int = Query(..., description="Minimum total score")):
    query = """
    SELECT COUNT(T1.cds) FROM satscores AS T1
    INNER JOIN schools AS T2 ON T1.cds = T2.CDSCode
    WHERE T2.MailCity = ? AND (T1.AvgScrRead + T1.AvgScrMath + T1.AvgScrWrite) >= ?
    """
    cursor.execute(query, (mail_city, min_score))
    return cursor.fetchall()

# Endpoint to get test takers for specific mail city
@app.get("/v1/bird/california_schools/test_takers_for_mail_city", summary="Get test takers for specific mail city")
async def get_test_takers_for_mail_city(mail_city: str = Query(..., description="Mail city")):
    query = """
    SELECT T1.NumTstTakr FROM satscores AS T1
    INNER JOIN schools AS T2 ON T1.cds = T2.CDSCode
    WHERE T2.MailCity = ?
    """
    cursor.execute(query, (mail_city,))
    return cursor.fetchall()

# Endpoint to get school and mail zip for specific admin
@app.get("/v1/bird/california_schools/school_and_mail_zip_by_admin", summary="Get school and mail zip for specific admin")
async def get_school_and_mail_zip_by_admin(adm_fname: str = Query(..., description="Admin first name"), adm_lname: str = Query(..., description="Admin last name")):
    query = """
    SELECT School, MailZip FROM schools
    WHERE AdmFName1 = ? AND AdmLName1 = ?
    """
    cursor.execute(query, (adm_fname, adm_lname))
    return cursor.fetchall()

# Endpoint to get school count ratio for specific counties
@app.get("/v1/bird/california_schools/school_count_ratio_by_counties", summary="Get school count ratio for specific counties")
async def get_school_count_ratio_by_counties(county1: str = Query(..., description="First county name"), county2: str = Query(..., description="Second county name")):
    query = """
    SELECT CAST(SUM(CASE WHEN County = ? THEN 1 ELSE 0 END) AS REAL) / SUM(CASE WHEN County = ? THEN 1 ELSE 0 END) FROM schools
    WHERE MailState = 'CA'
    """
    cursor.execute(query, (county1, county2))
    return cursor.fetchall()

# Endpoint to get school count for specific city, state, and status type
@app.get("/v1/bird/california_schools/school_count_for_city_state_and_status_type", summary="Get school count for specific city, state, and status type")
async def get_school_count_for_city_state_and_status_type(city: str = Query(..., description="City name"), state: str = Query(..., description="State name"), status_type: str = Query(..., description="Status type")):
    query = """
    SELECT COUNT(CDSCode) FROM schools
    WHERE City = ? AND MailState = ? AND StatusType = ?
    """
    cursor.execute(query, (city, state, status_type))
    return cursor.fetchall()

# Endpoint to get school phone and extension for top writing scores
@app.get("/v1/bird/california_schools/school_phone_and_extension_for_top_writing_scores", summary="Get school phone and extension for top writing scores")
async def get_school_phone_and_extension_for_top_writing_scores(limit: int = Query(332, description="Limit the number of results"), offset: int = Query(1, description="Offset the results")):
    query = """
    SELECT T2.Phone, T2.Ext FROM satscores AS T1
    INNER JOIN schools AS T2 ON T1.cds = T2.CDSCode
    ORDER BY T1.AvgScrWrite DESC
    LIMIT ? OFFSET ?
    """
    cursor.execute(query, (limit, offset))
    return cursor.fetchall()

# Endpoint to get school phone, extension, and name for specific zip
@app.get("/v1/bird/california_schools/school_phone_extension_and_name_by_zip", summary="Get school phone, extension, and name for specific zip")
async def get_school_phone_extension_and_name_by_zip(zip_code: str = Query(..., description="Zip code")):
    query = """
    SELECT Phone, Ext, School FROM schools
    WHERE Zip = ?
    """
    cursor.execute(query, (zip_code,))
    return cursor.fetchall()

# Endpoint to get school website for specific admin names
@app.get("/v1/bird/california_schools/school_website_by_admin_names", summary="Get school website for specific admin names")
async def get_school_website_by_admin_names(adm_fname1: str = Query(..., description="First admin first name"), adm_lname1: str = Query(..., description="First admin last name"), adm_fname2: str = Query(..., description="Second admin first name"), adm_lname2: str = Query(..., description="Second admin last name")):
    query = """
    SELECT Website FROM schools
    WHERE (AdmFName1 = ? AND AdmLName1 = ?) OR (AdmFName1 = ? AND AdmLName1 = ?)
    """
    cursor.execute(query, (adm_fname1, adm_lname1, adm_fname2, adm_lname2))
    return cursor.fetchall()

# Endpoint to get website for a given county, virtual status, and charter status
@app.get("/v1/bird/california_schools/website", summary="Get website for a given county, virtual status, and charter status")
async def get_website(county: str = Query(..., description="County name"),
                      virtual: str = Query(..., description="Virtual status"),
                      charter: int = Query(..., description="Charter status")):
    query = f"SELECT Website FROM schools WHERE County = ? AND Virtual = ? AND Charter = ?"
    cursor.execute(query, (county, virtual, charter))
    result = cursor.fetchall()
    return result

# Endpoint to get count of schools for a given DOC, charter status, and city
@app.get("/v1/bird/california_schools/school_count_by_doc_charter_city", summary="Get count of schools for a given DOC, charter status, and city")
async def get_school_count_by_doc_charter_city(doc: int = Query(..., description="DOC number"),
                                               charter: int = Query(..., description="Charter status"),
                                               city: str = Query(..., description="City name")):
    query = f"SELECT COUNT(School) FROM schools WHERE DOC = ? AND Charter = ? AND City = ?"
    cursor.execute(query, (doc, charter, city))
    result = cursor.fetchall()
    return result

# Endpoint to get count of schools with specific free meal count percentage
@app.get("/v1/bird/california_schools/school_count_by_free_meal_percentage", summary="Get count of schools with specific free meal count percentage")
async def get_school_count_by_free_meal_percentage(county: str = Query(..., description="County name"),
                                                   charter: int = Query(..., description="Charter status"),
                                                   percentage: float = Query(..., description="Free meal count percentage")):
    query = f"""
    SELECT COUNT(T2.School)
    FROM frpm AS T1
    INNER JOIN schools AS T2 ON T1.CDSCode = T2.CDSCode
    WHERE T2.County = ? AND T2.Charter = ? AND CAST(T1.`Free Meal Count (K-12)` AS REAL) * 100 / T1.`Enrollment (K-12)` < ?
    """
    cursor.execute(query, (county, charter, percentage))
    result = cursor.fetchall()
    return result

# Endpoint to get admin names and school details for a given charter number
@app.get("/v1/bird/california_schools/admin_names_and_school_details", summary="Get admin names and school details for a given charter number")
async def get_admin_names_and_school_details(charter_num: str = Query(..., description="Charter number")):
    query = f"SELECT AdmFName1, AdmLName1, School, City FROM schools WHERE Charter = 1 AND CharterNum = ?"
    cursor.execute(query, (charter_num,))
    result = cursor.fetchall()
    return result

# Endpoint to get count of schools for a given charter number and mail city
@app.get("/v1/bird/california_schools/school_count_by_charter_num_and_mail_city", summary="Get count of schools for a given charter number and mail city")
async def get_school_count_by_charter_num_and_mail_city(charter_num: str = Query(..., description="Charter number"),
                                                        mail_city: str = Query(..., description="Mail city")):
    query = f"SELECT COUNT(*) FROM schools WHERE CharterNum = ? AND MailCity = ?"
    cursor.execute(query, (charter_num, mail_city))
    result = cursor.fetchall()
    return result

# Endpoint to get funding type percentage for a given county and charter status
@app.get("/v1/bird/california_schools/funding_type_percentage", summary="Get funding type percentage for a given county and charter status")
async def get_funding_type_percentage(county: str = Query(..., description="County name"),
                                      charter: int = Query(..., description="Charter status")):
    query = f"""
    SELECT CAST(SUM(CASE WHEN FundingType = 'Locally funded' THEN 1 ELSE 0 END) AS REAL) * 100 /
           SUM(CASE WHEN FundingType != 'Locally funded' THEN 1 ELSE 0 END)
    FROM schools WHERE County = ? AND Charter = ?
    """
    cursor.execute(query, (county, charter))
    result = cursor.fetchall()
    return result

# Endpoint to get count of schools opened between specific years for a given county and funding type
@app.get("/v1/bird/california_schools/school_count_by_open_date_range", summary="Get count of schools opened between specific years for a given county and funding type")
async def get_school_count_by_open_date_range(start_year: int = Query(..., description="Start year"),
                                              end_year: int = Query(..., description="End year"),
                                              county: str = Query(..., description="County name"),
                                              funding_type: str = Query(..., description="Funding type")):
    query = f"""
    SELECT COUNT(School)
    FROM schools
    WHERE strftime('%Y', OpenDate) BETWEEN ? AND ? AND County = ? AND FundingType = ?
    """
    cursor.execute(query, (start_year, end_year, county, funding_type))
    result = cursor.fetchall()
    return result

# Endpoint to get count of schools closed in a specific year for a given city and DOC type
@app.get("/v1/bird/california_schools/school_count_by_closed_date", summary="Get count of schools closed in a specific year for a given city and DOC type")
async def get_school_count_by_closed_date(year: int = Query(..., description="Year"),
                                          city: str = Query(..., description="City name"),
                                          doc_type: str = Query(..., description="DOC type")):
    query = f"""
    SELECT COUNT(School)
    FROM schools
    WHERE strftime('%Y', ClosedDate) = ? AND City = ? AND DOCType = ?
    """
    cursor.execute(query, (year, city, doc_type))
    result = cursor.fetchall()
    return result

# Endpoint to get county with the highest number of closed schools between specific years
@app.get("/v1/bird/california_schools/county_with_highest_closed_schools", summary="Get county with the highest number of closed schools between specific years")
async def get_county_with_highest_closed_schools(start_year: int = Query(..., description="Start year"),
                                                 end_year: int = Query(..., description="End year"),
                                                 status_type: str = Query(..., description="Status type"),
                                                 soc: int = Query(..., description="SOC number")):
    query = f"""
    SELECT County
    FROM schools
    WHERE strftime('%Y', ClosedDate) BETWEEN ? AND ? AND StatusType = ? AND SOC = ?
    GROUP BY County
    ORDER BY COUNT(School) DESC
    LIMIT 1
    """
    cursor.execute(query, (start_year, end_year, status_type, soc))
    result = cursor.fetchall()
    return result

# Endpoint to get NCESDist for a given SOC number
@app.get("/v1/bird/california_schools/ncesdist_by_soc", summary="Get NCESDist for a given SOC number")
async def get_ncesdist_by_soc(soc: int = Query(..., description="SOC number")):
    query = f"SELECT NCESDist FROM schools WHERE SOC = ?"
    cursor.execute(query, (soc,))
    result = cursor.fetchall()
    return result

# Endpoint to get count of schools for a given status type, SOC number, and county
@app.get("/v1/bird/california_schools/school_count_by_status_soc_county", summary="Get count of schools for a given status type, SOC number, and county")
async def get_school_count_by_status_soc_county(status_type: str = Query(..., description="Status type"),
                                                soc: int = Query(..., description="SOC number"),
                                                county: str = Query(..., description="County name")):
    query = f"""
    SELECT COUNT(School)
    FROM schools
    WHERE (StatusType = ? OR StatusType = ?) AND SOC = ? AND County = ?
    """
    cursor.execute(query, (status_type, status_type, soc, county))
    result = cursor.fetchall()
    return result

# Endpoint to get district code for a given city and magnet status
@app.get("/v1/bird/california_schools/district_code_by_city_magnet", summary="Get district code for a given city and magnet status")
async def get_district_code_by_city_magnet(city: str = Query(..., description="City name"),
                                           magnet: int = Query(..., description="Magnet status")):
    query = f"""
    SELECT T1.`District Code`
    FROM frpm AS T1
    INNER JOIN schools AS T2 ON T1.CDSCode = T2.CDSCode
    WHERE T2.City = ? AND T2.Magnet = ?
    """
    cursor.execute(query, (city, magnet))
    result = cursor.fetchall()
    return result

# Endpoint to get enrollment for a given EdOpsCode, city, and academic year range
@app.get("/v1/bird/california_schools/enrollment_by_edopscode_city_year", summary="Get enrollment for a given EdOpsCode, city, and academic year range")
async def get_enrollment_by_edopscode_city_year(edopscode: str = Query(..., description="EdOpsCode"),
                                                city: str = Query(..., description="City name"),
                                                start_year: int = Query(..., description="Start year"),
                                                end_year: int = Query(..., description="End year")):
    query = f"""
    SELECT T1.`Enrollment (Ages 5-17)`
    FROM frpm AS T1
    INNER JOIN schools AS T2 ON T1.CDSCode = T2.CDSCode
    WHERE T2.EdOpsCode = ? AND T2.City = ? AND T1.`Academic Year` BETWEEN ? AND ?
    """
    cursor.execute(query, (edopscode, city, start_year, end_year))
    result = cursor.fetchall()
    return result

# Endpoint to get FRPM count for a given mail street and SOC type
@app.get("/v1/bird/california_schools/frpm_count_by_mailstreet_soctype", summary="Get FRPM count for a given mail street and SOC type")
async def get_frpm_count_by_mailstreet_soctype(mail_street: str = Query(..., description="Mail street"),
                                               soctype: str = Query(..., description="SOC type")):
    query = f"""
    SELECT T1.`FRPM Count (Ages 5-17)`
    FROM frpm AS T1
    INNER JOIN schools AS T2 ON T1.CDSCode = T2.CDSCode
    WHERE T2.MailStreet = ? AND T2.SOCType = ?
    """
    cursor.execute(query, (mail_street, soctype))
    result = cursor.fetchall()
    return result

# Endpoint to get minimum low grade for a given NCESDist and EdOpsCode
@app.get("/v1/bird/california_schools/min_low_grade_by_ncesdist_edopscode", summary="Get minimum low grade for a given NCESDist and EdOpsCode")
async def get_min_low_grade_by_ncesdist_edopscode(ncesdist: str = Query(..., description="NCESDist"),
                                                  edopscode: str = Query(..., description="EdOpsCode")):
    query = f"""
    SELECT MIN(T1.`Low Grade`)
    FROM frpm AS T1
    INNER JOIN schools AS T2 ON T1.CDSCode = T2.CDSCode
    WHERE T2.NCESDist = ? AND T2.EdOpsCode = ?
    """
    cursor.execute(query, (ncesdist, edopscode))
    result = cursor.fetchall()
    return result

# Endpoint to get EILName and school for a given NSLP provision status and county code
@app.get("/v1/bird/california_schools/eilname_school_by_nslp_countycode", summary="Get EILName and school for a given NSLP provision status and county code")
async def get_eilname_school_by_nslp_countycode(nslp_status: str = Query(..., description="NSLP provision status"),
                                                county_code: int = Query(..., description="County code")):
    query = f"""
    SELECT T2.EILName, T2.School
    FROM frpm AS T1
    INNER JOIN schools AS T2 ON T1.CDSCode = T2.CDSCode
    WHERE T1.`NSLP Provision Status` = ? AND T1.`County Code` = ?
    """
    cursor.execute(query, (nslp_status, county_code))
    result = cursor.fetchall()
    return result

# Endpoint to get city for a given NSLP provision status, county, low grade, high grade, and EILCode
@app.get("/v1/bird/california_schools/city_by_nslp_county_grades_eilcode", summary="Get city for a given NSLP provision status, county, low grade, high grade, and EILCode")
async def get_city_by_nslp_county_grades_eilcode(nslp_status: str = Query(..., description="NSLP provision status"),
                                                 county: str = Query(..., description="County name"),
                                                 low_grade: int = Query(..., description="Low grade"),
                                                 high_grade: int = Query(..., description="High grade"),
                                                 eilcode: str = Query(..., description="EILCode")):
    query = f"""
    SELECT T2.City
    FROM frpm AS T1
    INNER JOIN schools AS T2 ON T1.CDSCode = T2.CDSCode
    WHERE T1.`NSLP Provision Status` = ? AND T2.County = ? AND T1.`Low Grade` = ? AND T1.`High Grade` = ? AND T2.EILCode = ?
    """
    cursor.execute(query, (nslp_status, county, low_grade, high_grade, eilcode))
    result = cursor.fetchall()
    return result

# Endpoint to get school and FRPM percentage for a given county and GSserved
@app.get("/v1/bird/california_schools/school_frpm_percentage_by_county_gsserved", summary="Get school and FRPM percentage for a given county and GSserved")
async def get_school_frpm_percentage_by_county_gsserved(county: str = Query(..., description="County name"),
                                                        gsserved: str = Query(..., description="GSserved")):
    query = f"""
    SELECT T2.School, T1.`FRPM Count (Ages 5-17)` * 100 / T1.`Enrollment (Ages 5-17)`
    FROM frpm AS T1
    INNER JOIN schools AS T2 ON T1.CDSCode = T2.CDSCode
    WHERE T2.County = ? AND T2.GSserved = ?
    """
    cursor.execute(query, (county, gsserved))
    result = cursor.fetchall()
    return result

# Endpoint to get GSserved for a given city
@app.get("/v1/bird/california_schools/gsserved_by_city", summary="Get GSserved for a given city")
async def get_gsserved_by_city(city: str = Query(..., description="City name")):
    query = f"SELECT GSserved FROM schools WHERE City = ? GROUP BY GSserved ORDER BY COUNT(GSserved) DESC LIMIT 1"
    cursor.execute(query, (city,))
    result = cursor.fetchall()
    return result

# Endpoint to get county and virtual count for given counties and virtual status
@app.get("/v1/bird/california_schools/county_virtual_count", summary="Get county and virtual count for given counties and virtual status")
async def get_county_virtual_count(county1: str = Query(..., description="County 1"),
                                   county2: str = Query(..., description="County 2"),
                                   virtual: str = Query(..., description="Virtual status")):
    query = f"""
    SELECT County, COUNT(Virtual)
    FROM schools
    WHERE (County = ? OR County = ?) AND Virtual = ?
    GROUP BY County
    ORDER BY COUNT(Virtual) DESC
    LIMIT 1
    """
    cursor.execute(query, (county1, county2, virtual))
    result = cursor.fetchall()
    return result

# Endpoint to get school type, school name, and latitude for a given order
@app.get("/v1/bird/california_schools/school_type_name_latitude", summary="Get school type, school name, and latitude for a given order")
async def get_school_type_name_latitude(order: str = Query(..., description="Order by latitude")):
    query = f"""
    SELECT T1.`School Type`, T1.`School Name`, T2.Latitude
    FROM frpm AS T1
    INNER JOIN schools AS T2 ON T1.CDSCode = T2.CDSCode
    ORDER BY T2.Latitude {order}
    LIMIT 1
    """
    cursor.execute(query, (order,))
    result = cursor.fetchall()
    return result

# Endpoint to get city, low grade, and school name for a given state
@app.get("/v1/bird/california_schools/city_lowgrade_schoolname_by_state", summary="Get city, low grade, and school name for a given state")
async def get_city_lowgrade_schoolname_by_state(state: str = Query(..., description="State name")):
    query = f"""
    SELECT T2.City, T1.`Low Grade`, T1.`School Name`
    FROM frpm AS T1
    INNER JOIN schools AS T2 ON T1.CDSCode = T2.CDSCode
    WHERE T2.State = ?
    ORDER BY T2.Latitude ASC
    LIMIT 1
    """
    cursor.execute(query, (state,))
    result = cursor.fetchall()
    return result

# Endpoint to get GSoffered for a given order
@app.get("/v1/bird/california_schools/gs_offered_by_order", summary="Get GSoffered for a given order")
async def get_gs_offered_by_order(order: str = Query(..., description="Order by longitude")):
    query = f"SELECT GSoffered FROM schools ORDER BY ABS(longitude) {order} LIMIT 1"
    cursor.execute(query, (order,))
    result = cursor.fetchall()
    return result

# Endpoint to get city and CDSCode count for a given magnet status, GSoffered, and NSLP provision status
@app.get("/v1/bird/california_schools/city_cdscode_count_by_magnet_gs_nslp", summary="Get city and CDSCode count for a given magnet status, GSoffered, and NSLP provision status")
async def get_city_cdscode_count_by_magnet_gs_nslp(magnet: int = Query(..., description="Magnet status"),
                                                   gsoffered: str = Query(..., description="GSoffered"),
                                                   nslp_status: str = Query(..., description="NSLP provision status")):
    query = f"""
    SELECT T2.City, COUNT(T2.CDSCode)
    FROM frpm AS T1
    INNER JOIN schools AS T2 ON T1.CDSCode = T2.CDSCode
    WHERE T2.Magnet = ? AND T2.GSoffered = ? AND T1.`NSLP Provision Status` = ?
    GROUP BY T2.City
    """
    cursor.execute(query, (magnet, gsoffered, nslp_status))
    result = cursor.fetchall()
    return result

# Endpoint to get distinct admin first names and districts for a given admin first name count
@app.get("/v1/bird/california_schools/distinct_admin_names_districts", summary="Get distinct admin first names and districts for a given admin first name count")
async def get_distinct_admin_names_districts(count: int = Query(..., description="Admin first name count")):
    query = f"""
    SELECT DISTINCT T1.AdmFName1, T1.District
    FROM schools AS T1
    INNER JOIN (
        SELECT admfname1
        FROM schools
        GROUP BY admfname1
        ORDER BY COUNT(admfname1) DESC
        LIMIT ?
    ) AS T2 ON T1.AdmFName1 = T2.admfname1
    """
    cursor.execute(query, (count,))
    result = cursor.fetchall()
    return result

# Endpoint to get free meal count percentage and district code for a given admin first name
@app.get("/v1/bird/california_schools/free_meal_percentage_district_code", summary="Get free meal count percentage and district code for a given admin first name")
async def get_free_meal_percentage_district_code(admin_first_name: str = Query(..., description="Admin first name")):
    query = f"""
    SELECT T1.`Free Meal Count (K-12)` * 100 / T1.`Enrollment (K-12)`, T1.`District Code`
    FROM frpm AS T1
    INNER JOIN schools AS T2 ON T1.CDSCode = T2.CDSCode
    WHERE T2.AdmFName1 = ?
    """
    cursor.execute(query, (admin_first_name,))
    result = cursor.fetchall()
    return result

# Endpoint to get admin last name, district, county, and school for a given charter number
@app.get("/v1/bird/california_schools/admin_details_by_charter_num", summary="Get admin last name, district, county, and school for a given charter number")
async def get_admin_details_by_charter_num(charter_num: str = Query(..., description="Charter number")):
    query = f"SELECT AdmLName1, District, County, School FROM schools WHERE CharterNum = ?"
    cursor.execute(query, (charter_num,))
    result = cursor.fetchall()
    return result

# Endpoint to get admin emails for a given county, city, DOC, open date range, and SOC
@app.get("/v1/bird/california_schools/admin_emails_by_county_city_doc_opendate_soc", summary="Get admin emails for a given county, city, DOC, open date range, and SOC")
async def get_admin_emails_by_county_city_doc_opendate_soc(county: str = Query(..., description="County name"),
                                                           city: str = Query(..., description="City name"),
                                                           doc: int = Query(..., description="DOC number"),
                                                           start_year: int = Query(..., description="Start year"),
                                                           end_year: int = Query(..., description="End year"),
                                                           soc: int = Query(..., description="SOC number")):
    query = f"""
    SELECT T2.AdmEmail1, T2.AdmEmail2
    FROM frpm AS T1
    INNER JOIN schools AS T2 ON T1.CDSCode = T2.CDSCode
    WHERE T2.County = ? AND T2.City = ? AND T2.DOC = ? AND strftime('%Y', T2.OpenDate) BETWEEN ? AND ? AND T2.SOC = ?
    """
    cursor.execute(query, (county, city, doc, start_year, end_year, soc))
    result = cursor.fetchall()
    return result

# Endpoint to get admin email and school for a given order
@app.get("/v1/bird/california_schools/admin_email_school_by_order", summary="Get admin email and school for a given order")
async def get_admin_email_school_by_order(order: str = Query(..., description="Order by NumGE1500")):
    query = f"""
    SELECT T2.AdmEmail1, T2.School
    FROM satscores AS T1
    INNER JOIN schools AS T2 ON T1.cds = T2.CDSCode
    ORDER BY T1.NumGE1500 {order}
    LIMIT 1
    """
    cursor.execute(query, (order,))
    result = cursor.fetchall()
    return result
