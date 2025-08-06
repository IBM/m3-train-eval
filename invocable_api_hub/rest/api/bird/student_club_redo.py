
from fastapi import APIRouter, Query
import sqlite3

app = APIRouter()

# Database connection
conn = sqlite3.connect('invocable_api_hub/db/bird/test/student_club.sqlite', check_same_thread=False)
cursor = conn.cursor()

# Endpoint to get major name for a given member
@app.get("/v1/bird/student_club/major_name", summary="Get major name for a given member")
async def get_major_name(first_name: str = Query(..., description="First name of the member"),
                         last_name: str = Query(..., description="Last name of the member")):
    query = """
    SELECT T2.major_name
    FROM member AS T1
    INNER JOIN major AS T2 ON T1.link_to_major = T2.major_id
    WHERE T1.first_name = ? AND T1.last_name = ?
    """
    cursor.execute(query, (first_name, last_name))
    result = cursor.fetchall()
    return result

# Endpoint to get count of members in a specific college
@app.get("/v1/bird/student_club/member_count_by_college", summary="Get count of members in a specific college")
async def get_member_count_by_college(college: str = Query(..., description="Name of the college")):
    query = """
    SELECT COUNT(T1.member_id)
    FROM member AS T1
    INNER JOIN major AS T2 ON T1.link_to_major = T2.major_id
    WHERE T2.college = ?
    """
    cursor.execute(query, (college,))
    result = cursor.fetchall()
    return result

# Endpoint to get members in a specific department
@app.get("/v1/bird/student_club/members_by_department", summary="Get members in a specific department")
async def get_members_by_department(department: str = Query(..., description="Name of the department")):
    query = """
    SELECT T1.first_name, T1.last_name
    FROM member AS T1
    INNER JOIN major AS T2 ON T1.link_to_major = T2.major_id
    WHERE T2.department = ?
    """
    cursor.execute(query, (department,))
    result = cursor.fetchall()
    return result

# Endpoint to get count of events by event name
@app.get("/v1/bird/student_club/event_count_by_name", summary="Get count of events by event name")
async def get_event_count_by_name(event_name: str = Query(..., description="Name of the event")):
    query = """
    SELECT COUNT(T1.event_id)
    FROM event AS T1
    INNER JOIN attendance AS T2 ON T1.event_id = T2.link_to_event
    WHERE T1.event_name = ?
    """
    cursor.execute(query, (event_name,))
    result = cursor.fetchall()
    return result

# Endpoint to get phone numbers of attendees for a specific event
@app.get("/v1/bird/student_club/attendee_phones_by_event", summary="Get phone numbers of attendees for a specific event")
async def get_attendee_phones_by_event(event_name: str = Query(..., description="Name of the event")):
    query = """
    SELECT T3.phone
    FROM event AS T1
    INNER JOIN attendance AS T2 ON T1.event_id = T2.link_to_event
    INNER JOIN member AS T3 ON T2.link_to_member = T3.member_id
    WHERE T1.event_name = ?
    """
    cursor.execute(query, (event_name,))
    result = cursor.fetchall()
    return result

# Endpoint to get count of events by event name and t-shirt size
@app.get("/v1/bird/student_club/event_count_by_name_and_tshirt_size", summary="Get count of events by event name and t-shirt size")
async def get_event_count_by_name_and_tshirt_size(event_name: str = Query(..., description="Name of the event"),
                                                  t_shirt_size: str = Query(..., description="T-shirt size")):
    query = """
    SELECT COUNT(T1.event_id)
    FROM event AS T1
    INNER JOIN attendance AS T2 ON T1.event_id = T2.link_to_event
    INNER JOIN member AS T3 ON T2.link_to_member = T3.member_id
    WHERE T1.event_name = ? AND T3.t_shirt_size = ?
    """
    cursor.execute(query, (event_name, t_shirt_size))
    result = cursor.fetchall()
    return result

# Endpoint to get the most attended event
@app.get("/v1/bird/student_club/most_attended_event", summary="Get the most attended event")
async def get_most_attended_event():
    query = """
    SELECT T1.event_name
    FROM event AS T1
    INNER JOIN attendance AS T2 ON T1.event_id = T2.link_to_event
    GROUP BY T1.event_name
    ORDER BY COUNT(T2.link_to_event) DESC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get college by member position
@app.get("/v1/bird/student_club/college_by_position", summary="Get college by member position")
async def get_college_by_position(position: str = Query(..., description="Position of the member")):
    query = """
    SELECT T2.college
    FROM member AS T1
    INNER JOIN major AS T2 ON T1.link_to_major = T2.major_id
    WHERE T1.position LIKE ?
    """
    cursor.execute(query, (position,))
    result = cursor.fetchall()
    return result

# Endpoint to get events attended by a specific member
@app.get("/v1/bird/student_club/events_by_member", summary="Get events attended by a specific member")
async def get_events_by_member(first_name: str = Query(..., description="First name of the member"),
                               last_name: str = Query(..., description="Last name of the member")):
    query = """
    SELECT T1.event_name
    FROM event AS T1
    INNER JOIN attendance AS T2 ON T1.event_id = T2.link_to_event
    INNER JOIN member AS T3 ON T2.link_to_member = T3.member_id
    WHERE T3.first_name = ? AND T3.last_name = ?
    """
    cursor.execute(query, (first_name, last_name))
    result = cursor.fetchall()
    return result

# Endpoint to get count of events attended by a specific member in a specific year
@app.get("/v1/bird/student_club/event_count_by_member_and_year", summary="Get count of events attended by a specific member in a specific year")
async def get_event_count_by_member_and_year(first_name: str = Query(..., description="First name of the member"),
                                             last_name: str = Query(..., description="Last name of the member"),
                                             year: int = Query(..., description="Year of the event")):
    query = """
    SELECT COUNT(T1.event_id)
    FROM event AS T1
    INNER JOIN attendance AS T2 ON T1.event_id = T2.link_to_event
    INNER JOIN member AS T3 ON T2.link_to_member = T3.member_id
    WHERE T3.first_name = ? AND T3.last_name = ? AND SUBSTR(T1.event_date, 1, 4) = ?
    """
    cursor.execute(query, (first_name, last_name, str(year)))
    result = cursor.fetchall()
    return result

# Endpoint to get events with more than 10 attendees excluding meetings
@app.get("/v1/bird/student_club/events_with_more_than_10_attendees_excluding_meetings", summary="Get events with more than 10 attendees excluding meetings")
async def get_events_with_more_than_10_attendees_excluding_meetings():
    query = """
    SELECT T1.event_name
    FROM event AS T1
    INNER JOIN attendance AS T2 ON T1.event_id = T2.link_to_event
    GROUP BY T1.event_id
    HAVING COUNT(T2.link_to_event) > 10
    EXCEPT
    SELECT T1.event_name
    FROM event AS T1
    WHERE T1.type = 'Meeting'
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get events with more than 20 attendees excluding fundraisers
@app.get("/v1/bird/student_club/events_with_more_than_20_attendees_excluding_fundraisers", summary="Get events with more than 20 attendees excluding fundraisers")
async def get_events_with_more_than_20_attendees_excluding_fundraisers():
    query = """
    SELECT T1.event_name
    FROM event AS T1
    INNER JOIN attendance AS T2 ON T1.event_id = T2.link_to_event
    GROUP BY T1.event_id
    HAVING COUNT(T2.link_to_event) > 20
    EXCEPT
    SELECT T1.event_name
    FROM event AS T1
    WHERE T1.type = 'Fundraiser'
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get average attendance for meetings in a specific year
@app.get("/v1/bird/student_club/average_attendance_for_meetings_in_year", summary="Get average attendance for meetings in a specific year")
async def get_average_attendance_for_meetings_in_year(year: int = Query(..., description="Year of the event")):
    query = """
    SELECT CAST(COUNT(T2.link_to_event) AS REAL) / COUNT(DISTINCT T2.link_to_event)
    FROM event AS T1
    INNER JOIN attendance AS T2 ON T1.event_id = T2.link_to_event
    WHERE SUBSTR(T1.event_date, 1, 4) = ? AND T1.type = 'Meeting'
    """
    cursor.execute(query, (str(year),))
    result = cursor.fetchall()
    return result

# Endpoint to get the most expensive expense
@app.get("/v1/bird/student_club/most_expensive_expense", summary="Get the most expensive expense")
async def get_most_expensive_expense():
    query = """
    SELECT expense_description
    FROM expense
    ORDER BY cost DESC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get count of members in a specific major
@app.get("/v1/bird/student_club/member_count_by_major", summary="Get count of members in a specific major")
async def get_member_count_by_major(major_name: str = Query(..., description="Name of the major")):
    query = """
    SELECT COUNT(T1.member_id)
    FROM member AS T1
    INNER JOIN major AS T2 ON T1.link_to_major = T2.major_id
    WHERE T2.major_name = ?
    """
    cursor.execute(query, (major_name,))
    result = cursor.fetchall()
    return result

# Endpoint to get members who attended a specific event
@app.get("/v1/bird/student_club/members_by_event", summary="Get members who attended a specific event")
async def get_members_by_event(event_name: str = Query(..., description="Name of the event")):
    query = """
    SELECT T1.first_name, T1.last_name
    FROM member AS T1
    INNER JOIN attendance AS T2 ON T1.member_id = T2.link_to_member
    INNER JOIN event AS T3 ON T2.link_to_event = T3.event_id
    WHERE T3.event_name = ?
    """
    cursor.execute(query, (event_name,))
    result = cursor.fetchall()
    return result

# Endpoint to get last names of members in a specific major
@app.get("/v1/bird/student_club/last_names_by_major", summary="Get last names of members in a specific major")
async def get_last_names_by_major(major_name: str = Query(..., description="Name of the major")):
    query = """
    SELECT T1.last_name
    FROM member AS T1
    INNER JOIN major AS T2 ON T1.link_to_major = T2.major_id
    WHERE T2.major_name = ?
    """
    cursor.execute(query, (major_name,))
    result = cursor.fetchall()
    return result

# Endpoint to get county by member name
@app.get("/v1/bird/student_club/county_by_member_name", summary="Get county by member name")
async def get_county_by_member_name(first_name: str = Query(..., description="First name of the member"),
                                    last_name: str = Query(..., description="Last name of the member")):
    query = """
    SELECT T2.county
    FROM member AS T1
    INNER JOIN zip_code AS T2 ON T1.zip = T2.zip_code
    WHERE T1.first_name = ? AND T1.last_name = ?
    """
    cursor.execute(query, (first_name, last_name))
    result = cursor.fetchall()
    return result

# Endpoint to get college by member name
@app.get("/v1/bird/student_club/college_by_member_name", summary="Get college by member name")
async def get_college_by_member_name(first_name: str = Query(..., description="First name of the member"),
                                     last_name: str = Query(..., description="Last name of the member")):
    query = """
    SELECT T2.college
    FROM member AS T1
    INNER JOIN major AS T2 ON T1.link_to_major = T2.major_id
    WHERE T1.first_name = ? AND T1.last_name = ?
    """
    cursor.execute(query, (first_name, last_name))
    result = cursor.fetchall()
    return result

# Endpoint to get income amount by member position
@app.get("/v1/bird/student_club/income_by_position", summary="Get income amount by member position")
async def get_income_by_position(position: str = Query(..., description="Position of the member")):
    query = """
    SELECT T2.amount
    FROM member AS T1
    INNER JOIN income AS T2 ON T1.member_id = T2.link_to_member
    WHERE T1.position = ?
    """
    cursor.execute(query, (position,))
    result = cursor.fetchall()
    return result

# Endpoint to get spent amount for a given event, category, and month
@app.get("/v1/bird/student_club/spent", summary="Get spent amount for a given event, category, and month")
async def get_spent_amount(event_name: str = Query(..., description="Name of the event"),
                           category: str = Query(..., description="Category of the budget"),
                           month: str = Query(..., description="Month of the event (MM)")):
    query = """
    SELECT T2.spent
    FROM event AS T1
    INNER JOIN budget AS T2 ON T1.event_id = T2.link_to_event
    WHERE T1.event_name = ? AND T2.category = ? AND SUBSTR(T1.event_date, 6, 2) = ?
    """
    cursor.execute(query, (event_name, category, month))
    result = cursor.fetchall()
    return result

# Endpoint to get city and state for a given position
@app.get("/v1/bird/student_club/member_location", summary="Get city and state for a given position")
async def get_member_location(position: str = Query(..., description="Position of the member")):
    query = """
    SELECT T2.city, T2.state
    FROM member AS T1
    INNER JOIN zip_code AS T2 ON T1.zip = T2.zip_code
    WHERE T1.position = ?
    """
    cursor.execute(query, (position,))
    result = cursor.fetchall()
    return result

# Endpoint to get member names for a given state
@app.get("/v1/bird/student_club/member_names", summary="Get member names for a given state")
async def get_member_names(state: str = Query(..., description="State of the member")):
    query = """
    SELECT T1.first_name, T1.last_name
    FROM member AS T1
    INNER JOIN zip_code AS T2 ON T1.zip = T2.zip_code
    WHERE T2.state = ?
    """
    cursor.execute(query, (state,))
    result = cursor.fetchall()
    return result

# Endpoint to get department for a given last name
@app.get("/v1/bird/student_club/member_department", summary="Get department for a given last name")
async def get_member_department(last_name: str = Query(..., description="Last name of the member")):
    query = """
    SELECT T2.department
    FROM member AS T1
    INNER JOIN major AS T2 ON T1.link_to_major = T2.major_id
    WHERE T1.last_name = ?
    """
    cursor.execute(query, (last_name,))
    result = cursor.fetchall()
    return result

# Endpoint to get total amount for a given event
@app.get("/v1/bird/student_club/total_amount", summary="Get total amount for a given event")
async def get_total_amount(event_name: str = Query(..., description="Name of the event")):
    query = """
    SELECT SUM(T2.amount)
    FROM event AS T1
    INNER JOIN budget AS T2 ON T1.event_id = T2.link_to_event
    WHERE T1.event_name = ?
    """
    cursor.execute(query, (event_name,))
    result = cursor.fetchall()
    return result

# Endpoint to get approved status for a given event and date
@app.get("/v1/bird/student_club/approved_status", summary="Get approved status for a given event and date")
async def get_approved_status(event_name: str = Query(..., description="Name of the event"),
                              event_date: str = Query(..., description="Date of the event (YYYY-MM-DD)")):
    query = """
    SELECT T3.approved
    FROM event AS T1
    INNER JOIN budget AS T2 ON T1.event_id = T2.link_to_event
    INNER JOIN expense AS T3 ON T2.budget_id = T3.link_to_budget
    WHERE T1.event_name = ? AND T1.event_date LIKE ?
    """
    cursor.execute(query, (event_name, event_date + '%'))
    result = cursor.fetchall()
    return result

# Endpoint to get average cost for a given member and months
@app.get("/v1/bird/student_club/average_cost", summary="Get average cost for a given member and months")
async def get_average_cost(last_name: str = Query(..., description="Last name of the member"),
                           first_name: str = Query(..., description="First name of the member"),
                           month1: str = Query(..., description="First month (MM)"),
                           month2: str = Query(..., description="Second month (MM)")):
    query = """
    SELECT AVG(T2.cost)
    FROM member AS T1
    INNER JOIN expense AS T2 ON T1.member_id = T2.link_to_member
    WHERE T1.last_name = ? AND T1.first_name = ? AND (SUBSTR(T2.expense_date, 6, 2) = ? OR SUBSTR(T2.expense_date, 6, 2) = ?)
    """
    cursor.execute(query, (last_name, first_name, month1, month2))
    result = cursor.fetchall()
    return result

# Endpoint to get difference in spent amount between two years
@app.get("/v1/bird/student_club/spent_difference", summary="Get difference in spent amount between two years")
async def get_spent_difference(year1: str = Query(..., description="First year (YYYY)"),
                               year2: str = Query(..., description="Second year (YYYY)")):
    query = """
    SELECT SUM(CASE WHEN SUBSTR(T1.event_date, 1, 4) = ? THEN T2.spent ELSE 0 END) - SUM(CASE WHEN SUBSTR(T1.event_date, 1, 4) = ? THEN T2.spent ELSE 0 END) AS num
    FROM event AS T1
    INNER JOIN budget AS T2 ON T1.event_id = T2.link_to_event
    """
    cursor.execute(query, (year1, year2))
    result = cursor.fetchall()
    return result

# Endpoint to get location for a given event
@app.get("/v1/bird/student_club/event_location", summary="Get location for a given event")
async def get_event_location(event_name: str = Query(..., description="Name of the event")):
    query = """
    SELECT location
    FROM event
    WHERE event_name = ?
    """
    cursor.execute(query, (event_name,))
    result = cursor.fetchall()
    return result

# Endpoint to get cost for a given expense description and date
@app.get("/v1/bird/student_club/expense_cost", summary="Get cost for a given expense description and date")
async def get_expense_cost(expense_description: str = Query(..., description="Description of the expense"),
                           expense_date: str = Query(..., description="Date of the expense (YYYY-MM-DD)")):
    query = """
    SELECT cost
    FROM expense
    WHERE expense_description = ? AND expense_date = ?
    """
    cursor.execute(query, (expense_description, expense_date))
    result = cursor.fetchall()
    return result

# Endpoint to get remaining amount for a given category and maximum amount
@app.get("/v1/bird/student_club/remaining_amount", summary="Get remaining amount for a given category and maximum amount")
async def get_remaining_amount(category: str = Query(..., description="Category of the budget")):
    query = """
    SELECT remaining
    FROM budget
    WHERE category = ? AND amount = (SELECT MAX(amount) FROM budget WHERE category = ?)
    """
    cursor.execute(query, (category, category))
    result = cursor.fetchall()
    return result

# Endpoint to get notes for a given source and date received
@app.get("/v1/bird/student_club/income_notes", summary="Get notes for a given source and date received")
async def get_income_notes(source: str = Query(..., description="Source of the income"),
                           date_received: str = Query(..., description="Date received (YYYY-MM-DD)")):
    query = """
    SELECT notes
    FROM income
    WHERE source = ? AND date_received = ?
    """
    cursor.execute(query, (source, date_received))
    result = cursor.fetchall()
    return result

# Endpoint to get count of majors for a given college
@app.get("/v1/bird/student_club/major_count", summary="Get count of majors for a given college")
async def get_major_count(college: str = Query(..., description="Name of the college")):
    query = """
    SELECT COUNT(major_name)
    FROM major
    WHERE college = ?
    """
    cursor.execute(query, (college,))
    result = cursor.fetchall()
    return result

# Endpoint to get phone number for a given first name and last name
@app.get("/v1/bird/student_club/member_phone", summary="Get phone number for a given first name and last name")
async def get_member_phone(first_name: str = Query(..., description="First name of the member"),
                           last_name: str = Query(..., description="Last name of the member")):
    query = """
    SELECT phone
    FROM member
    WHERE first_name = ? AND last_name = ?
    """
    cursor.execute(query, (first_name, last_name))
    result = cursor.fetchall()
    return result

# Endpoint to get county for a given first name and last name
@app.get("/v1/bird/student_club/member_county", summary="Get county for a given first name and last name")
async def get_member_county(first_name: str = Query(..., description="First name of the member"),
                            last_name: str = Query(..., description="Last name of the member")):
    query = """
    SELECT T2.county
    FROM member AS T1
    INNER JOIN zip_code AS T2 ON T1.zip = T2.zip_code
    WHERE T1.first_name = ? AND T1.last_name = ?
    """
    cursor.execute(query, (first_name, last_name))
    result = cursor.fetchall()
    return result

# Endpoint to get count of events for a given event name and remaining amount
@app.get("/v1/bird/student_club/event_count", summary="Get count of events for a given event name and remaining amount")
async def get_event_count(event_name: str = Query(..., description="Name of the event"),
                          remaining: int = Query(..., description="Remaining amount")):
    query = """
    SELECT COUNT(T2.event_id)
    FROM budget AS T1
    INNER JOIN event AS T2 ON T1.link_to_event = T2.event_id
    WHERE T2.event_name = ? AND T1.remaining < ?
    """
    cursor.execute(query, (event_name, remaining))
    result = cursor.fetchall()
    return result

# Endpoint to get total amount for a given event
@app.get("/v1/bird/student_club/total_budget_amount", summary="Get total amount for a given event")
async def get_total_budget_amount(event_name: str = Query(..., description="Name of the event")):
    query = """
    SELECT SUM(T1.amount)
    FROM budget AS T1
    INNER JOIN event AS T2 ON T1.link_to_event = T2.event_id
    WHERE T2.event_name = ?
    """
    cursor.execute(query, (event_name,))
    result = cursor.fetchall()
    return result

# Endpoint to get event status for a given expense description and date
@app.get("/v1/bird/student_club/event_status", summary="Get event status for a given expense description and date")
async def get_event_status(expense_description: str = Query(..., description="Description of the expense"),
                           expense_date: str = Query(..., description="Date of the expense (YYYY-MM-DD)")):
    query = """
    SELECT T1.event_status
    FROM budget AS T1
    INNER JOIN expense AS T2 ON T1.budget_id = T2.link_to_budget
    WHERE T2.expense_description = ? AND T2.expense_date = ?
    """
    cursor.execute(query, (expense_description, expense_date))
    result = cursor.fetchall()
    return result

# Endpoint to get major name for a given first name and last name
@app.get("/v1/bird/student_club/member_major", summary="Get major name for a given first name and last name")
async def get_member_major(first_name: str = Query(..., description="First name of the member"),
                           last_name: str = Query(..., description="Last name of the member")):
    query = """
    SELECT T2.major_name
    FROM member AS T1
    INNER JOIN major AS T2 ON T1.link_to_major = T2.major_id
    WHERE T1.first_name = ? AND T1.last_name = ?
    """
    cursor.execute(query, (first_name, last_name))
    result = cursor.fetchall()
    return result

# Endpoint to get count of members with specific major and t-shirt size
@app.get("/v1/bird/student_club/member_count", summary="Get count of members with specific major and t-shirt size")
async def get_member_count(major_name: str = Query(..., description="Name of the major"), t_shirt_size: str = Query(..., description="T-shirt size")):
    query = """
    SELECT COUNT(T1.member_id) FROM member AS T1
    INNER JOIN major AS T2 ON T1.link_to_major = T2.major_id
    WHERE T2.major_name = ? AND T1.t_shirt_size = ?
    """
    cursor.execute(query, (major_name, t_shirt_size))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get zip code type for a specific member
@app.get("/v1/bird/student_club/zip_code_type", summary="Get zip code type for a specific member")
async def get_zip_code_type(first_name: str = Query(..., description="First name of the member"), last_name: str = Query(..., description="Last name of the member")):
    query = """
    SELECT T2.type FROM member AS T1
    INNER JOIN zip_code AS T2 ON T1.zip = T2.zip_code
    WHERE T1.first_name = ? AND T1.last_name = ?
    """
    cursor.execute(query, (first_name, last_name))
    result = cursor.fetchone()
    return {"type": result[0]}

# Endpoint to get major name for a specific position
@app.get("/v1/bird/student_club/major_name", summary="Get major name for a specific position")
async def get_major_name(position: str = Query(..., description="Position of the member")):
    query = """
    SELECT T2.major_name FROM member AS T1
    INNER JOIN major AS T2 ON T1.link_to_major = T2.major_id
    WHERE T1.position = ?
    """
    cursor.execute(query, (position,))
    result = cursor.fetchone()
    return {"major_name": result[0]}

# Endpoint to get state for a specific member
@app.get("/v1/bird/student_club/member_state", summary="Get state for a specific member")
async def get_member_state(first_name: str = Query(..., description="First name of the member"), last_name: str = Query(..., description="Last name of the member")):
    query = """
    SELECT T2.state FROM member AS T1
    INNER JOIN zip_code AS T2 ON T1.zip = T2.zip_code
    WHERE T1.first_name = ? AND T1.last_name = ?
    """
    cursor.execute(query, (first_name, last_name))
    result = cursor.fetchone()
    return {"state": result[0]}

# Endpoint to get department for a specific position
@app.get("/v1/bird/student_club/member_department", summary="Get department for a specific position")
async def get_member_department(position: str = Query(..., description="Position of the member")):
    query = """
    SELECT T2.department FROM member AS T1
    INNER JOIN major AS T2 ON T1.link_to_major = T2.major_id
    WHERE T1.position = ?
    """
    cursor.execute(query, (position,))
    result = cursor.fetchone()
    return {"department": result[0]}

# Endpoint to get date received for a specific member and income source
@app.get("/v1/bird/student_club/income_date_received", summary="Get date received for a specific member and income source")
async def get_income_date_received(first_name: str = Query(..., description="First name of the member"), last_name: str = Query(..., description="Last name of the member"), source: str = Query(..., description="Source of income")):
    query = """
    SELECT T2.date_received FROM member AS T1
    INNER JOIN income AS T2 ON T1.member_id = T2.link_to_member
    WHERE T1.first_name = ? AND T1.last_name = ? AND T2.source = ?
    """
    cursor.execute(query, (first_name, last_name, source))
    result = cursor.fetchone()
    return {"date_received": result[0]}

# Endpoint to get member names for a specific income source
@app.get("/v1/bird/student_club/member_names", summary="Get member names for a specific income source")
async def get_member_names(source: str = Query(..., description="Source of income")):
    query = """
    SELECT T1.first_name, T1.last_name FROM member AS T1
    INNER JOIN income AS T2 ON T1.member_id = T2.link_to_member
    WHERE T2.source = ?
    ORDER BY T2.date_received
    LIMIT 1
    """
    cursor.execute(query, (source,))
    result = cursor.fetchone()
    return {"first_name": result[0], "last_name": result[1]}

# Endpoint to get budget ratio for specific event names and category
@app.get("/v1/bird/student_club/budget_ratio", summary="Get budget ratio for specific event names and category")
async def get_budget_ratio(category: str = Query(..., description="Category of the budget"), type: str = Query(..., description="Type of the event")):
    query = """
    SELECT CAST(SUM(CASE WHEN T2.event_name = 'Yearly Kickoff' THEN T1.amount ELSE 0 END) AS REAL) / SUM(CASE WHEN T2.event_name = 'October Meeting' THEN T1.amount ELSE 0 END)
    FROM budget AS T1
    INNER JOIN event AS T2 ON T1.link_to_event = T2.event_id
    WHERE T1.category = ? AND T2.type = ?
    """
    cursor.execute(query, (category, type))
    result = cursor.fetchone()
    return {"ratio": result[0]}

# Endpoint to get budget percentage for a specific event name
@app.get("/v1/bird/student_club/budget_percentage", summary="Get budget percentage for a specific event name")
async def get_budget_percentage(event_name: str = Query(..., description="Name of the event")):
    query = """
    SELECT CAST(SUM(CASE WHEN T1.category = 'Parking' THEN T1.amount ELSE 0 END) AS REAL) * 100 / SUM(T1.amount)
    FROM budget AS T1
    INNER JOIN event AS T2 ON T1.link_to_event = T2.event_id
    WHERE T2.event_name = ?
    """
    cursor.execute(query, (event_name,))
    result = cursor.fetchone()
    return {"percentage": result[0]}

# Endpoint to get total cost for a specific expense description
@app.get("/v1/bird/student_club/total_cost", summary="Get total cost for a specific expense description")
async def get_total_cost(expense_description: str = Query(..., description="Description of the expense")):
    query = """
    SELECT SUM(cost) FROM expense WHERE expense_description = ?
    """
    cursor.execute(query, (expense_description,))
    result = cursor.fetchone()
    return {"total_cost": result[0]}

# Endpoint to get count of cities for a specific county and state
@app.get("/v1/bird/student_club/city_count", summary="Get count of cities for a specific county and state")
async def get_city_count(county: str = Query(..., description="Name of the county"), state: str = Query(..., description="Name of the state")):
    query = """
    SELECT COUNT(city) FROM zip_code WHERE county = ? AND state = ?
    """
    cursor.execute(query, (county, state))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get department for a specific college
@app.get("/v1/bird/student_club/department_by_college", summary="Get department for a specific college")
async def get_department_by_college(college: str = Query(..., description="Name of the college")):
    query = """
    SELECT department FROM major WHERE college = ?
    """
    cursor.execute(query, (college,))
    result = cursor.fetchone()
    return {"department": result[0]}

# Endpoint to get city, county, and state for a specific member
@app.get("/v1/bird/student_club/member_location", summary="Get city, county, and state for a specific member")
async def get_member_location(first_name: str = Query(..., description="First name of the member"), last_name: str = Query(..., description="Last name of the member")):
    query = """
    SELECT T2.city, T2.county, T2.state FROM member AS T1
    INNER JOIN zip_code AS T2 ON T1.zip = T2.zip_code
    WHERE T1.first_name = ? AND T1.last_name = ?
    """
    cursor.execute(query, (first_name, last_name))
    result = cursor.fetchone()
    return {"city": result[0], "county": result[1], "state": result[2]}

# Endpoint to get expense description for a specific budget
@app.get("/v1/bird/student_club/expense_description", summary="Get expense description for a specific budget")
async def get_expense_description():
    query = """
    SELECT T2.expense_description FROM budget AS T1
    INNER JOIN expense AS T2 ON T1.budget_id = T2.link_to_budget
    ORDER BY T1.remaining
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"expense_description": result[0]}

# Endpoint to get distinct member IDs for a specific event
@app.get("/v1/bird/student_club/distinct_member_ids", summary="Get distinct member IDs for a specific event")
async def get_distinct_member_ids(event_name: str = Query(..., description="Name of the event")):
    query = """
    SELECT DISTINCT T3.member_id FROM event AS T1
    INNER JOIN attendance AS T2 ON T1.event_id = T2.link_to_event
    INNER JOIN member AS T3 ON T2.link_to_member = T3.member_id
    WHERE T1.event_name = ?
    """
    cursor.execute(query, (event_name,))
    result = cursor.fetchall()
    return {"member_ids": [row[0] for row in result]}

# Endpoint to get college with the highest number of majors
@app.get("/v1/bird/student_club/top_college", summary="Get college with the highest number of majors")
async def get_top_college():
    query = """
    SELECT T2.college FROM member AS T1
    INNER JOIN major AS T2 ON T1.link_to_major = T2.major_id
    GROUP BY T2.major_id
    ORDER BY COUNT(T2.college) DESC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"college": result[0]}

# Endpoint to get major name for a specific phone number
@app.get("/v1/bird/student_club/major_name_by_phone", summary="Get major name for a specific phone number")
async def get_major_name_by_phone(phone: str = Query(..., description="Phone number of the member")):
    query = """
    SELECT T2.major_name FROM member AS T1
    INNER JOIN major AS T2 ON T1.link_to_major = T2.major_id
    WHERE T1.phone = ?
    """
    cursor.execute(query, (phone,))
    result = cursor.fetchone()
    return {"major_name": result[0]}

# Endpoint to get event name with the highest budget amount
@app.get("/v1/bird/student_club/top_event", summary="Get event name with the highest budget amount")
async def get_top_event():
    query = """
    SELECT T2.event_name FROM budget AS T1
    INNER JOIN event AS T2 ON T1.link_to_event = T2.event_id
    ORDER BY T1.amount DESC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"event_name": result[0]}

# Endpoint to get expense details for a specific position
@app.get("/v1/bird/student_club/expense_details", summary="Get expense details for a specific position")
async def get_expense_details(position: str = Query(..., description="Position of the member")):
    query = """
    SELECT T2.expense_id, T2.expense_description FROM member AS T1
    INNER JOIN expense AS T2 ON T1.member_id = T2.link_to_member
    WHERE T1.position = ?
    """
    cursor.execute(query, (position,))
    result = cursor.fetchall()
    return {"expenses": [{"expense_id": row[0], "expense_description": row[1]} for row in result]}

# Endpoint to get count of attendees for a specific event
@app.get("/v1/bird/student_club/attendee_count", summary="Get count of attendees for a specific event")
async def get_attendee_count(event_name: str = Query(..., description="Name of the event")):
    query = """
    SELECT COUNT(T2.link_to_member) FROM event AS T1
    INNER JOIN attendance AS T2 ON T1.event_id = T2.link_to_event
    WHERE T1.event_name = ?
    """
    cursor.execute(query, (event_name,))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get date received for a given member
@app.get("/v1/bird/student_club/date_received", summary="Get date received for a given member")
async def get_date_received(first_name: str = Query(..., description="First name of the member"),
                            last_name: str = Query(..., description="Last name of the member")):
    query = """
    SELECT T2.date_received
    FROM member AS T1
    INNER JOIN income AS T2 ON T1.member_id = T2.link_to_member
    WHERE T1.first_name = ? AND T1.last_name = ?
    """
    cursor.execute(query, (first_name, last_name))
    result = cursor.fetchall()
    return result

# Endpoint to get count of members in a given state
@app.get("/v1/bird/student_club/member_count_by_state", summary="Get count of members in a given state")
async def get_member_count_by_state(state: str = Query(..., description="State name")):
    query = """
    SELECT COUNT(T2.member_id)
    FROM zip_code AS T1
    INNER JOIN member AS T2 ON T1.zip_code = T2.zip
    WHERE T1.state = ?
    """
    cursor.execute(query, (state,))
    result = cursor.fetchall()
    return result

# Endpoint to get count of events attended by a member with a given phone number
@app.get("/v1/bird/student_club/event_count_by_phone", summary="Get count of events attended by a member with a given phone number")
async def get_event_count_by_phone(phone: str = Query(..., description="Phone number of the member")):
    query = """
    SELECT COUNT(T2.link_to_event)
    FROM member AS T1
    INNER JOIN attendance AS T2 ON T1.member_id = T2.link_to_member
    WHERE T1.phone = ?
    """
    cursor.execute(query, (phone,))
    result = cursor.fetchall()
    return result

# Endpoint to get members in a given department
@app.get("/v1/bird/student_club/members_by_department", summary="Get members in a given department")
async def get_members_by_department(department: str = Query(..., description="Department name")):
    query = """
    SELECT T1.first_name, T1.last_name
    FROM member AS T1
    INNER JOIN major AS T2 ON T1.link_to_major = T2.major_id
    WHERE T2.department = ?
    """
    cursor.execute(query, (department,))
    result = cursor.fetchall()
    return result

# Endpoint to get event with the highest budget spent to amount ratio
@app.get("/v1/bird/student_club/event_with_highest_budget_ratio", summary="Get event with the highest budget spent to amount ratio")
async def get_event_with_highest_budget_ratio(status: str = Query(..., description="Event status")):
    query = """
    SELECT T2.event_name
    FROM budget AS T1
    INNER JOIN event AS T2 ON T1.link_to_event = T2.event_id
    WHERE T2.status = ?
    ORDER BY T1.spent / T1.amount DESC
    LIMIT 1
    """
    cursor.execute(query, (status,))
    result = cursor.fetchall()
    return result

# Endpoint to get count of members with a given position
@app.get("/v1/bird/student_club/member_count_by_position", summary="Get count of members with a given position")
async def get_member_count_by_position(position: str = Query(..., description="Position name")):
    query = """
    SELECT COUNT(member_id)
    FROM member
    WHERE position = ?
    """
    cursor.execute(query, (position,))
    result = cursor.fetchall()
    return result

# Endpoint to get maximum spent amount
@app.get("/v1/bird/student_club/max_spent", summary="Get maximum spent amount")
async def get_max_spent():
    query = """
    SELECT MAX(spent)
    FROM budget
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get count of events of a given type in a specific year
@app.get("/v1/bird/student_club/event_count_by_type_and_year", summary="Get count of events of a given type in a specific year")
async def get_event_count_by_type_and_year(type: str = Query(..., description="Event type"),
                                           year: int = Query(..., description="Year")):
    query = """
    SELECT COUNT(event_id)
    FROM event
    WHERE type = ? AND SUBSTR(event_date, 1, 4) = ?
    """
    cursor.execute(query, (type, str(year)))
    result = cursor.fetchall()
    return result

# Endpoint to get total spent amount for a given category
@app.get("/v1/bird/student_club/total_spent_by_category", summary="Get total spent amount for a given category")
async def get_total_spent_by_category(category: str = Query(..., description="Category name")):
    query = """
    SELECT SUM(spent)
    FROM budget
    WHERE category = ?
    """
    cursor.execute(query, (category,))
    result = cursor.fetchall()
    return result

# Endpoint to get members who attended more than a certain number of events
@app.get("/v1/bird/student_club/members_by_attendance", summary="Get members who attended more than a certain number of events")
async def get_members_by_attendance(min_events: int = Query(..., description="Minimum number of events attended")):
    query = """
    SELECT T1.first_name, T1.last_name
    FROM member AS T1
    INNER JOIN attendance AS T2 ON T1.member_id = T2.link_to_member
    GROUP BY T2.link_to_member
    HAVING COUNT(T2.link_to_event) > ?
    """
    cursor.execute(query, (min_events,))
    result = cursor.fetchall()
    return result

# Endpoint to get members in a specific major who attended a specific event
@app.get("/v1/bird/student_club/members_by_major_and_event", summary="Get members in a specific major who attended a specific event")
async def get_members_by_major_and_event(event_name: str = Query(..., description="Event name"),
                                         major_name: str = Query(..., description="Major name")):
    query = """
    SELECT T2.first_name, T2.last_name
    FROM major AS T1
    INNER JOIN member AS T2 ON T1.major_id = T2.link_to_major
    INNER JOIN attendance AS T3 ON T2.member_id = T3.link_to_member
    INNER JOIN event AS T4 ON T3.link_to_event = T4.event_id
    WHERE T4.event_name = ? AND T1.major_name = ?
    """
    cursor.execute(query, (event_name, major_name))
    result = cursor.fetchall()
    return result

# Endpoint to get members in a specific city and state
@app.get("/v1/bird/student_club/members_by_city_and_state", summary="Get members in a specific city and state")
async def get_members_by_city_and_state(city: str = Query(..., description="City name"),
                                        state: str = Query(..., description="State name")):
    query = """
    SELECT T1.first_name, T1.last_name
    FROM member AS T1
    INNER JOIN zip_code AS T2 ON T1.zip = T2.zip_code
    WHERE T2.city = ? AND T2.state = ?
    """
    cursor.execute(query, (city, state))
    result = cursor.fetchall()
    return result

# Endpoint to get income amount for a given member
@app.get("/v1/bird/student_club/income_by_member", summary="Get income amount for a given member")
async def get_income_by_member(first_name: str = Query(..., description="First name of the member"),
                               last_name: str = Query(..., description="Last name of the member")):
    query = """
    SELECT T2.amount
    FROM member AS T1
    INNER JOIN income AS T2 ON T1.member_id = T2.link_to_member
    WHERE T1.first_name = ? AND T1.last_name = ?
    """
    cursor.execute(query, (first_name, last_name))
    result = cursor.fetchall()
    return result

# Endpoint to get members with income greater than a certain amount
@app.get("/v1/bird/student_club/members_by_income", summary="Get members with income greater than a certain amount")
async def get_members_by_income(min_amount: float = Query(..., description="Minimum income amount")):
    query = """
    SELECT T1.first_name, T1.last_name
    FROM member AS T1
    INNER JOIN income AS T2 ON T1.member_id = T2.link_to_member
    WHERE T2.amount > ?
    """
    cursor.execute(query, (min_amount,))
    result = cursor.fetchall()
    return result

# Endpoint to get total cost for a specific event
@app.get("/v1/bird/student_club/total_cost_by_event", summary="Get total cost for a specific event")
async def get_total_cost_by_event(event_name: str = Query(..., description="Event name")):
    query = """
    SELECT SUM(T3.cost)
    FROM event AS T1
    INNER JOIN budget AS T2 ON T1.event_id = T2.link_to_event
    INNER JOIN expense AS T3 ON T2.budget_id = T3.link_to_budget
    WHERE T1.event_name = ?
    """
    cursor.execute(query, (event_name,))
    result = cursor.fetchall()
    return result

# Endpoint to get members who incurred expenses for a specific event
@app.get("/v1/bird/student_club/members_by_event_expenses", summary="Get members who incurred expenses for a specific event")
async def get_members_by_event_expenses(event_name: str = Query(..., description="Event name")):
    query = """
    SELECT T4.first_name, T4.last_name
    FROM event AS T1
    INNER JOIN budget AS T2 ON T1.event_id = T2.link_to_event
    INNER JOIN expense AS T3 ON T2.budget_id = T3.link_to_budget
    INNER JOIN member AS T4 ON T3.link_to_member = T4.member_id
    WHERE T1.event_name = ?
    """
    cursor.execute(query, (event_name,))
    result = cursor.fetchall()
    return result

# Endpoint to get member with the highest total income
@app.get("/v1/bird/student_club/member_with_highest_income", summary="Get member with the highest total income")
async def get_member_with_highest_income():
    query = """
    SELECT T1.first_name, T1.last_name, T2.source
    FROM member AS T1
    INNER JOIN income AS T2 ON T1.member_id = T2.link_to_member
    GROUP BY T1.first_name, T1.last_name, T2.source
    ORDER BY SUM(T2.amount) DESC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get event with the highest expense cost
@app.get("/v1/bird/student_club/event_with_highest_expense", summary="Get event with the highest expense cost")
async def get_event_with_highest_expense():
    query = """
    SELECT T1.event_name
    FROM event AS T1
    INNER JOIN budget AS T2 ON T1.event_id = T2.link_to_event
    INNER JOIN expense AS T3 ON T2.budget_id = T3.link_to_budget
    ORDER BY T3.cost DESC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get percentage of total cost for a specific event
@app.get("/v1/bird/student_club/percentage_of_total_cost_by_event", summary="Get percentage of total cost for a specific event")
async def get_percentage_of_total_cost_by_event(event_name: str = Query(..., description="Event name")):
    query = """
    SELECT CAST(SUM(CASE WHEN T1.event_name = ? THEN T3.cost ELSE 0 END) AS REAL) * 100 / SUM(T3.cost)
    FROM event AS T1
    INNER JOIN budget AS T2 ON T1.event_id = T2.link_to_event
    INNER JOIN expense AS T3 ON T2.budget_id = T3.link_to_budget
    """
    cursor.execute(query, (event_name,))
    result = cursor.fetchall()
    return result

# Endpoint to get ratio of members in two different majors
@app.get("/v1/bird/student_club/ratio_of_members_by_major", summary="Get ratio of members in two different majors")
async def get_ratio_of_members_by_major(major1: str = Query(..., description="First major name"),
                                        major2: str = Query(..., description="Second major name")):
    query = """
    SELECT SUM(CASE WHEN major_name = ? THEN 1 ELSE 0 END) / SUM(CASE WHEN major_name = ? THEN 1 ELSE 0 END) AS ratio
    FROM major
    """
    cursor.execute(query, (major1, major2))
    result = cursor.fetchall()
    return result

# Endpoint to get source from income within a date range
@app.get("/v1/bird/student_club/income/source", summary="Get source from income within a date range")
async def get_income_source(start_date: str = Query(..., description="Start date in YYYY-MM-DD format"),
                            end_date: str = Query(..., description="End date in YYYY-MM-DD format")):
    query = f"SELECT source FROM income WHERE date_received BETWEEN '{start_date}' and '{end_date}' ORDER BY source DESC LIMIT 1"
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get member details by position
@app.get("/v1/bird/student_club/member/details", summary="Get member details by position")
async def get_member_details(position: str = Query(..., description="Position of the member")):
    query = f"SELECT first_name, last_name, email FROM member WHERE position = '{position}'"
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get count of members in a specific major
@app.get("/v1/bird/student_club/major/member_count", summary="Get count of members in a specific major")
async def get_major_member_count(major_name: str = Query(..., description="Name of the major")):
    query = f"SELECT COUNT(T2.member_id) FROM major AS T1 INNER JOIN member AS T2 ON T1.major_id = T2.link_to_major WHERE T1.major_name = '{major_name}'"
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get count of attendees for a specific event in a specific year
@app.get("/v1/bird/student_club/event/attendance_count", summary="Get count of attendees for a specific event in a specific year")
async def get_event_attendance_count(event_name: str = Query(..., description="Name of the event"),
                                     year: str = Query(..., description="Year in YYYY format")):
    query = f"SELECT COUNT(T2.link_to_member) FROM event AS T1 INNER JOIN attendance AS T2 ON T1.event_id = T2.link_to_event WHERE T1.event_name = '{event_name}' AND SUBSTR(T1.event_date, 1, 4) = '{year}'"
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get count of events attended by a specific member
@app.get("/v1/bird/student_club/member/event_count", summary="Get count of events attended by a specific member")
async def get_member_event_count(first_name: str = Query(..., description="First name of the member"),
                                 last_name: str = Query(..., description="Last name of the member")):
    query = f"SELECT COUNT(T3.link_to_event), T1.major_name FROM major AS T1 INNER JOIN member AS T2 ON T1.major_id = T2.link_to_major INNER JOIN attendance AS T3 ON T2.member_id = T3.link_to_member WHERE T2.first_name = '{first_name}' AND T2.last_name = '{last_name}'"
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get average spent on a specific category with a specific event status
@app.get("/v1/bird/student_club/budget/average_spent", summary="Get average spent on a specific category with a specific event status")
async def get_average_spent(category: str = Query(..., description="Category of the budget"),
                            event_status: str = Query(..., description="Status of the event")):
    query = f"SELECT SUM(spent) / COUNT(spent) FROM budget WHERE category = '{category}' AND event_status = '{event_status}'"
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get event name with the highest spent in a specific category
@app.get("/v1/bird/student_club/budget/highest_spent_event", summary="Get event name with the highest spent in a specific category")
async def get_highest_spent_event(category: str = Query(..., description="Category of the budget")):
    query = f"SELECT T2.event_name FROM budget AS T1 INNER JOIN event AS T2 ON T1.link_to_event = T2.event_id WHERE T1.category = '{category}' ORDER BY T1.spent DESC LIMIT 1"
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to check if a member attended a specific event
@app.get("/v1/bird/student_club/member/event_attendance", summary="Check if a member attended a specific event")
async def check_member_event_attendance(first_name: str = Query(..., description="First name of the member"),
                                        last_name: str = Query(..., description="Last name of the member"),
                                        event_name: str = Query(..., description="Name of the event")):
    query = f"SELECT CASE WHEN T3.event_name = '{event_name}' THEN 'YES' END AS result FROM member AS T1 INNER JOIN attendance AS T2 ON T1.member_id = T2.link_to_member INNER JOIN event AS T3 ON T2.link_to_event = T3.event_id WHERE T1.first_name = '{first_name}' AND T1.last_name = '{last_name}'"
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get percentage of community service events in a specific year
@app.get("/v1/bird/student_club/event/community_service_percentage", summary="Get percentage of community service events in a specific year")
async def get_community_service_percentage(year: str = Query(..., description="Year in YYYY format")):
    query = f"SELECT CAST(SUM(CASE WHEN type = 'Community Service' THEN 1 ELSE 0 END) AS REAL) * 100 / COUNT(type) FROM event WHERE SUBSTR(event_date, 1, 4) = '{year}'"
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get cost of a specific expense for a specific event
@app.get("/v1/bird/student_club/event/expense_cost", summary="Get cost of a specific expense for a specific event")
async def get_expense_cost(event_name: str = Query(..., description="Name of the event"),
                           expense_description: str = Query(..., description="Description of the expense")):
    query = f"SELECT T3.cost FROM event AS T1 INNER JOIN budget AS T2 ON T1.event_id = T2.link_to_event INNER JOIN expense AS T3 ON T2.budget_id = T3.link_to_budget WHERE T1.event_name = '{event_name}' AND T3.expense_description = '{expense_description}'"
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get the most common t-shirt size
@app.get("/v1/bird/student_club/member/common_tshirt_size", summary="Get the most common t-shirt size")
async def get_common_tshirt_size():
    query = "SELECT t_shirt_size FROM member GROUP BY t_shirt_size ORDER BY COUNT(t_shirt_size) DESC LIMIT 1"
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get event with the highest negative remaining budget
@app.get("/v1/bird/student_club/budget/highest_negative_remaining", summary="Get event with the highest negative remaining budget")
async def get_highest_negative_remaining():
    query = "SELECT T2.event_name FROM budget AS T1 INNER JOIN event AS T2 ON T2.event_id = T1.link_to_event WHERE T1.event_status = 'Closed' AND T1.remaining < 0 ORDER BY T1.remaining LIMIT 1"
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get total cost for a specific event
@app.get("/v1/bird/student_club/event/total_cost", summary="Get total cost for a specific event")
async def get_total_cost(event_name: str = Query(..., description="Name of the event")):
    query = f"SELECT T1.type, SUM(T3.cost) FROM event AS T1 INNER JOIN budget AS T2 ON T1.event_id = T2.link_to_event INNER JOIN expense AS T3 ON T2.budget_id = T3.link_to_budget WHERE T1.event_name = '{event_name}'"
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get total amount spent by category for a specific event
@app.get("/v1/bird/student_club/event/total_amount_by_category", summary="Get total amount spent by category for a specific event")
async def get_total_amount_by_category(event_name: str = Query(..., description="Name of the event")):
    query = f"SELECT T2.category, SUM(T2.amount) FROM event AS T1 JOIN budget AS T2 ON T1.event_id = T2.link_to_event WHERE T1.event_name = '{event_name}' GROUP BY T2.category ORDER BY SUM(T2.amount) ASC"
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get budget ID with the highest amount in a specific category
@app.get("/v1/bird/student_club/budget/highest_amount", summary="Get budget ID with the highest amount in a specific category")
async def get_highest_amount(category: str = Query(..., description="Category of the budget")):
    query = f"SELECT budget_id FROM budget WHERE category = '{category}' AND amount = (SELECT MAX(amount) FROM budget)"
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get top 3 budget IDs with the highest amount in a specific category
@app.get("/v1/bird/student_club/budget/top_amounts", summary="Get top 3 budget IDs with the highest amount in a specific category")
async def get_top_amounts(category: str = Query(..., description="Category of the budget")):
    query = f"SELECT budget_id FROM budget WHERE category = '{category}' ORDER BY amount DESC LIMIT 3"
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get total cost for a specific expense description
@app.get("/v1/bird/student_club/expense/total_cost", summary="Get total cost for a specific expense description")
async def get_expense_total_cost(expense_description: str = Query(..., description="Description of the expense")):
    query = f"SELECT SUM(cost) FROM expense WHERE expense_description = '{expense_description}'"
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get total cost for a specific expense date
@app.get("/v1/bird/student_club/expense/total_cost_by_date", summary="Get total cost for a specific expense date")
async def get_expense_total_cost_by_date(expense_date: str = Query(..., description="Date of the expense in YYYY-MM-DD format")):
    query = f"SELECT SUM(cost) FROM expense WHERE expense_date = '{expense_date}'"
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get total cost for a specific member
@app.get("/v1/bird/student_club/member/total_cost", summary="Get total cost for a specific member")
async def get_member_total_cost(member_id: str = Query(..., description="ID of the member")):
    query = f"SELECT T1.first_name, T1.last_name, SUM(T2.cost) FROM member AS T1 INNER JOIN expense AS T2 ON T1.member_id = T2.link_to_member WHERE T1.member_id = '{member_id}'"
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get expense descriptions for a specific member
@app.get("/v1/bird/student_club/member/expense_descriptions", summary="Get expense descriptions for a specific member")
async def get_member_expense_descriptions(first_name: str = Query(..., description="First name of the member"),
                                          last_name: str = Query(..., description="Last name of the member")):
    query = f"SELECT T2.expense_description FROM member AS T1 INNER JOIN expense AS T2 ON T1.member_id = T2.link_to_member WHERE T1.first_name = '{first_name}' AND T1.last_name = '{last_name}'"
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get expense description for a given t-shirt size
@app.get("/v1/bird/student_club/expense_description", summary="Get expense description for a given t-shirt size")
async def get_expense_description(t_shirt_size: str = Query(..., description="T-shirt size")):
    query = "SELECT T2.expense_description FROM member AS T1 INNER JOIN expense AS T2 ON T1.member_id = T2.link_to_member WHERE T1.t_shirt_size = ?"
    cursor.execute(query, (t_shirt_size,))
    result = cursor.fetchall()
    return result

# Endpoint to get zip code for a given cost
@app.get("/v1/bird/student_club/zip_code", summary="Get zip code for a given cost")
async def get_zip_code(cost: float = Query(..., description="Cost")):
    query = "SELECT T1.zip FROM member AS T1 INNER JOIN expense AS T2 ON T1.member_id = T2.link_to_member WHERE T2.cost < ?"
    cursor.execute(query, (cost,))
    result = cursor.fetchall()
    return result

# Endpoint to get major name for a given first name and last name
@app.get("/v1/bird/student_club/major_name", summary="Get major name for a given first name and last name")
async def get_major_name(first_name: str = Query(..., description="First name"), last_name: str = Query(..., description="Last name")):
    query = "SELECT T1.major_name FROM major AS T1 INNER JOIN member AS T2 ON T1.major_id = T2.link_to_major WHERE T2.first_name = ? AND T2.last_name = ?"
    cursor.execute(query, (first_name, last_name))
    result = cursor.fetchall()
    return result

# Endpoint to get position for a given major name
@app.get("/v1/bird/student_club/position", summary="Get position for a given major name")
async def get_position(major_name: str = Query(..., description="Major name")):
    query = "SELECT T2.position FROM major AS T1 INNER JOIN member AS T2 ON T1.major_id = T2.link_to_major WHERE T1.major_name = ?"
    cursor.execute(query, (major_name,))
    result = cursor.fetchall()
    return result

# Endpoint to get count of members for a given major name and t-shirt size
@app.get("/v1/bird/student_club/member_count", summary="Get count of members for a given major name and t-shirt size")
async def get_member_count(major_name: str = Query(..., description="Major name"), t_shirt_size: str = Query(..., description="T-shirt size")):
    query = "SELECT COUNT(T2.member_id) FROM major AS T1 INNER JOIN member AS T2 ON T1.major_id = T2.link_to_major WHERE T1.major_name = ? AND T2.t_shirt_size = ?"
    cursor.execute(query, (major_name, t_shirt_size))
    result = cursor.fetchall()
    return result

# Endpoint to get event type for a given remaining budget
@app.get("/v1/bird/student_club/event_type", summary="Get event type for a given remaining budget")
async def get_event_type(remaining: float = Query(..., description="Remaining budget")):
    query = "SELECT T1.type FROM event AS T1 INNER JOIN budget AS T2 ON T1.event_id = T2.link_to_event WHERE T2.remaining > ?"
    cursor.execute(query, (remaining,))
    result = cursor.fetchall()
    return result

# Endpoint to get budget category for a given event location
@app.get("/v1/bird/student_club/budget_category", summary="Get budget category for a given event location")
async def get_budget_category(location: str = Query(..., description="Event location")):
    query = "SELECT T2.category FROM event AS T1 INNER JOIN budget AS T2 ON T1.event_id = T2.link_to_event WHERE T1.location = ?"
    cursor.execute(query, (location,))
    result = cursor.fetchall()
    return result

# Endpoint to get budget category for a given event date
@app.get("/v1/bird/student_club/budget_category_by_date", summary="Get budget category for a given event date")
async def get_budget_category_by_date(event_date: str = Query(..., description="Event date")):
    query = "SELECT T2.category FROM event AS T1 INNER JOIN budget AS T2 ON T1.event_id = T2.link_to_event WHERE T1.event_date = ?"
    cursor.execute(query, (event_date,))
    result = cursor.fetchall()
    return result

# Endpoint to get major name for a given position
@app.get("/v1/bird/student_club/major_name_by_position", summary="Get major name for a given position")
async def get_major_name_by_position(position: str = Query(..., description="Position")):
    query = "SELECT T1.major_name FROM major AS T1 INNER JOIN member AS T2 ON T1.major_id = T2.link_to_major WHERE T2.position = ?"
    cursor.execute(query, (position,))
    result = cursor.fetchall()
    return result

# Endpoint to get percentage of members in a given major
@app.get("/v1/bird/student_club/percentage_of_members", summary="Get percentage of members in a given major")
async def get_percentage_of_members(position: str = Query(..., description="Position")):
    query = "SELECT CAST(SUM(CASE WHEN T2.major_name = 'Business' THEN 1 ELSE 0 END) AS REAL) * 100 / COUNT(T1.member_id) FROM member AS T1 INNER JOIN major AS T2 ON T2.major_id = T1.link_to_major WHERE T1.position = ?"
    cursor.execute(query, (position,))
    result = cursor.fetchall()
    return result

# Endpoint to get distinct budget categories for a given event location
@app.get("/v1/bird/student_club/distinct_budget_categories", summary="Get distinct budget categories for a given event location")
async def get_distinct_budget_categories(location: str = Query(..., description="Event location")):
    query = "SELECT DISTINCT T2.category FROM event AS T1 INNER JOIN budget AS T2 ON T1.event_id = T2.link_to_event WHERE T1.location = ?"
    cursor.execute(query, (location,))
    result = cursor.fetchall()
    return result

# Endpoint to get count of incomes for a given amount
@app.get("/v1/bird/student_club/income_count", summary="Get count of incomes for a given amount")
async def get_income_count(amount: float = Query(..., description="Amount")):
    query = "SELECT COUNT(income_id) FROM income WHERE amount = ?"
    cursor.execute(query, (amount,))
    result = cursor.fetchall()
    return result

# Endpoint to get count of members for a given position and t-shirt size
@app.get("/v1/bird/student_club/member_count_by_position_and_size", summary="Get count of members for a given position and t-shirt size")
async def get_member_count_by_position_and_size(position: str = Query(..., description="Position"), t_shirt_size: str = Query(..., description="T-shirt size")):
    query = "SELECT COUNT(member_id) FROM member WHERE position = ? AND t_shirt_size = ?"
    cursor.execute(query, (position, t_shirt_size))
    result = cursor.fetchall()
    return result

# Endpoint to get count of majors for a given department and college
@app.get("/v1/bird/student_club/major_count", summary="Get count of majors for a given department and college")
async def get_major_count(department: str = Query(..., description="Department"), college: str = Query(..., description="College")):
    query = "SELECT COUNT(major_id) FROM major WHERE department = ? AND college = ?"
    cursor.execute(query, (department, college))
    result = cursor.fetchall()
    return result

# Endpoint to get member details for a given position and major name
@app.get("/v1/bird/student_club/member_details", summary="Get member details for a given position and major name")
async def get_member_details(position: str = Query(..., description="Position"), major_name: str = Query(..., description="Major name")):
    query = "SELECT T2.last_name, T1.department, T1.college FROM major AS T1 INNER JOIN member AS T2 ON T1.major_id = T2.link_to_major WHERE T2.position = ? AND T1.major_name = ?"
    cursor.execute(query, (position, major_name))
    result = cursor.fetchall()
    return result

# Endpoint to get distinct budget categories and event type for a given location, spent amount, and event type
@app.get("/v1/bird/student_club/distinct_budget_categories_and_event_type", summary="Get distinct budget categories and event type for a given location, spent amount, and event type")
async def get_distinct_budget_categories_and_event_type(location: str = Query(..., description="Event location"), spent: float = Query(..., description="Spent amount"), event_type: str = Query(..., description="Event type")):
    query = "SELECT DISTINCT T2.category, T1.type FROM event AS T1 INNER JOIN budget AS T2 ON T1.event_id = T2.link_to_event WHERE T1.location = ? AND T2.spent = ? AND T1.type = ?"
    cursor.execute(query, (location, spent, event_type))
    result = cursor.fetchall()
    return result

# Endpoint to get city and state for a given department and position
@app.get("/v1/bird/student_club/city_and_state", summary="Get city and state for a given department and position")
async def get_city_and_state(department: str = Query(..., description="Department"), position: str = Query(..., description="Position")):
    query = "SELECT city, state FROM member AS T1 INNER JOIN major AS T2 ON T2.major_id = T1.link_to_major INNER JOIN zip_code AS T3 ON T3.zip_code = T1.zip WHERE department = ? AND position = ?"
    cursor.execute(query, (department, position))
    result = cursor.fetchall()
    return result

# Endpoint to get event name for a given position, location, and event type
@app.get("/v1/bird/student_club/event_name", summary="Get event name for a given position, location, and event type")
async def get_event_name(position: str = Query(..., description="Position"), location: str = Query(..., description="Event location"), event_type: str = Query(..., description="Event type")):
    query = "SELECT T2.event_name FROM attendance AS T1 INNER JOIN event AS T2 ON T2.event_id = T1.link_to_event INNER JOIN member AS T3 ON T1.link_to_member = T3.member_id WHERE T3.position = ? AND T2.location = ? AND T2.type = ?"
    cursor.execute(query, (position, location, event_type))
    result = cursor.fetchall()
    return result

# Endpoint to get member details for a given expense date and expense description
@app.get("/v1/bird/student_club/member_details_by_expense", summary="Get member details for a given expense date and expense description")
async def get_member_details_by_expense(expense_date: str = Query(..., description="Expense date"), expense_description: str = Query(..., description="Expense description")):
    query = "SELECT T1.last_name, T1.position FROM member AS T1 INNER JOIN expense AS T2 ON T1.member_id = T2.link_to_member WHERE T2.expense_date = ? AND T2.expense_description = ?"
    cursor.execute(query, (expense_date, expense_description))
    result = cursor.fetchall()
    return result

# Endpoint to get member last name for a given event name and position
@app.get("/v1/bird/student_club/member_last_name", summary="Get member last name for a given event name and position")
async def get_member_last_name(event_name: str = Query(..., description="Event name"), position: str = Query(..., description="Position")):
    query = "SELECT T3.last_name FROM attendance AS T1 INNER JOIN event AS T2 ON T2.event_id = T1.link_to_event INNER JOIN member AS T3 ON T1.link_to_member = T3.member_id WHERE T2.event_name = ? AND T3.position = ?"
    cursor.execute(query, (event_name, position))
    result = cursor.fetchall()
    return result

# Endpoint to get percentage of members with a specific t-shirt size and position
@app.get("/v1/bird/student_club/member_percentage", summary="Get percentage of members with a specific t-shirt size and position")
async def get_member_percentage(position: str = Query(..., description="Position of the member"), t_shirt_size: str = Query(..., description="T-shirt size of the member")):
    query = f"""
    SELECT CAST(SUM(CASE WHEN T2.amount = 50 THEN 1.0 ELSE 0 END) AS REAL) * 100 / COUNT(T2.income_id)
    FROM member AS T1
    INNER JOIN income AS T2 ON T1.member_id = T2.link_to_member
    WHERE T1.position = ? AND T1.t_shirt_size = ?
    """
    cursor.execute(query, (position, t_shirt_size))
    result = cursor.fetchone()
    return {"percentage": result[0]}

# Endpoint to get distinct counties with a specific type
@app.get("/v1/bird/student_club/distinct_counties", summary="Get distinct counties with a specific type")
async def get_distinct_counties(type: str = Query(..., description="Type of zip code")):
    query = f"""
    SELECT DISTINCT county FROM zip_code WHERE type = ? AND county IS NOT NULL
    """
    cursor.execute(query, (type,))
    result = cursor.fetchall()
    return {"counties": [row[0] for row in result]}

# Endpoint to get zip codes with specific type, county, and state
@app.get("/v1/bird/student_club/zip_codes", summary="Get zip codes with specific type, county, and state")
async def get_zip_codes(type: str = Query(..., description="Type of zip code"), county: str = Query(..., description="County"), state: str = Query(..., description="State")):
    query = f"""
    SELECT zip_code FROM zip_code WHERE type = ? AND county = ? AND state = ?
    """
    cursor.execute(query, (type, county, state))
    result = cursor.fetchall()
    return {"zip_codes": [row[0] for row in result]}

# Endpoint to get distinct event names with specific type, date range, and status
@app.get("/v1/bird/student_club/distinct_event_names", summary="Get distinct event names with specific type, date range, and status")
async def get_distinct_event_names(type: str = Query(..., description="Type of event"), start_date: str = Query(..., description="Start date"), end_date: str = Query(..., description="End date"), status: str = Query(..., description="Status of event")):
    query = f"""
    SELECT DISTINCT event_name FROM event
    WHERE type = ? AND date(SUBSTR(event_date, 1, 10)) BETWEEN ? AND ? AND status = ?
    """
    cursor.execute(query, (type, start_date, end_date, status))
    result = cursor.fetchall()
    return {"event_names": [row[0] for row in result]}

# Endpoint to get distinct event links with cost greater than a specific value
@app.get("/v1/bird/student_club/distinct_event_links", summary="Get distinct event links with cost greater than a specific value")
async def get_distinct_event_links(cost: float = Query(..., description="Cost threshold")):
    query = f"""
    SELECT DISTINCT T3.link_to_event
    FROM expense AS T1
    INNER JOIN member AS T2 ON T1.link_to_member = T2.member_id
    INNER JOIN attendance AS T3 ON T2.member_id = T3.link_to_member
    WHERE T1.cost > ?
    """
    cursor.execute(query, (cost,))
    result = cursor.fetchall()
    return {"event_links": [row[0] for row in result]}

# Endpoint to get distinct member and event links with specific date range and approval status
@app.get("/v1/bird/student_club/member_event_links", summary="Get distinct member and event links with specific date range and approval status")
async def get_member_event_links(start_date: str = Query(..., description="Start date"), end_date: str = Query(..., description="End date"), approved: str = Query(..., description="Approval status")):
    query = f"""
    SELECT DISTINCT T1.link_to_member, T3.link_to_event
    FROM expense AS T1
    INNER JOIN member AS T2 ON T1.link_to_member = T2.member_id
    INNER JOIN attendance AS T3 ON T2.member_id = T3.link_to_member
    WHERE date(SUBSTR(T1.expense_date, 1, 10)) BETWEEN ? AND ? AND T1.approved = ?
    """
    cursor.execute(query, (start_date, end_date, approved))
    result = cursor.fetchall()
    return {"member_event_links": [{"member_link": row[0], "event_link": row[1]} for row in result]}

# Endpoint to get college for a specific major and first name
@app.get("/v1/bird/student_club/college_for_major", summary="Get college for a specific major and first name")
async def get_college_for_major(major_id: str = Query(..., description="Major ID"), first_name: str = Query(..., description="First name")):
    query = f"""
    SELECT T2.college
    FROM member AS T1
    INNER JOIN major AS T2 ON T2.major_id = T1.link_to_major
    WHERE T1.link_to_major = ? AND T1.first_name = ?
    """
    cursor.execute(query, (major_id, first_name))
    result = cursor.fetchone()
    return {"college": result[0]}

# Endpoint to get phone number for a specific major and college
@app.get("/v1/bird/student_club/phone_for_major", summary="Get phone number for a specific major and college")
async def get_phone_for_major(major_name: str = Query(..., description="Major name"), college: str = Query(..., description="College")):
    query = f"""
    SELECT T1.phone
    FROM member AS T1
    INNER JOIN major AS T2 ON T2.major_id = T1.link_to_major
    WHERE T2.major_name = ? AND T2.college = ?
    """
    cursor.execute(query, (major_name, college))
    result = cursor.fetchall()
    return {"phones": [row[0] for row in result]}

# Endpoint to get distinct emails with specific date range and cost threshold
@app.get("/v1/bird/student_club/distinct_emails", summary="Get distinct emails with specific date range and cost threshold")
async def get_distinct_emails(start_date: str = Query(..., description="Start date"), end_date: str = Query(..., description="End date"), cost: float = Query(..., description="Cost threshold")):
    query = f"""
    SELECT DISTINCT T1.email
    FROM member AS T1
    INNER JOIN expense AS T2 ON T1.member_id = T2.link_to_member
    WHERE date(SUBSTR(T2.expense_date, 1, 10)) BETWEEN ? AND ? AND T2.cost > ?
    """
    cursor.execute(query, (start_date, end_date, cost))
    result = cursor.fetchall()
    return {"emails": [row[0] for row in result]}

# Endpoint to get count of members with specific position, major name, and college
@app.get("/v1/bird/student_club/member_count", summary="Get count of members with specific position, major name, and college")
async def get_member_count(position: str = Query(..., description="Position of the member"), major_name: str = Query(..., description="Major name"), college: str = Query(..., description="College")):
    query = f"""
    SELECT COUNT(T1.member_id)
    FROM member AS T1
    INNER JOIN major AS T2 ON T2.major_id = T1.link_to_major
    WHERE T1.position = ? AND T2.major_name LIKE ? AND T2.college = ?
    """
    cursor.execute(query, (position, f"%{major_name}%", college))
    result = cursor.fetchone()
    return {"member_count": result[0]}

# Endpoint to get percentage of budgets with remaining less than 0
@app.get("/v1/bird/student_club/budget_percentage", summary="Get percentage of budgets with remaining less than 0")
async def get_budget_percentage():
    query = f"""
    SELECT CAST(SUM(CASE WHEN remaining < 0 THEN 1 ELSE 0 END) AS REAL) * 100 / COUNT(budget_id)
    FROM budget
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"percentage": result[0]}

# Endpoint to get event details with specific date range
@app.get("/v1/bird/student_club/event_details", summary="Get event details with specific date range")
async def get_event_details(start_date: str = Query(..., description="Start date"), end_date: str = Query(..., description="End date")):
    query = f"""
    SELECT event_id, location, status
    FROM event
    WHERE date(SUBSTR(event_date, 1, 10)) BETWEEN ? AND ?
    """
    cursor.execute(query, (start_date, end_date))
    result = cursor.fetchall()
    return {"event_details": [{"event_id": row[0], "location": row[1], "status": row[2]} for row in result]}

# Endpoint to get expense descriptions with average cost greater than a specific value
@app.get("/v1/bird/student_club/expense_descriptions", summary="Get expense descriptions with average cost greater than a specific value")
async def get_expense_descriptions(cost: float = Query(..., description="Cost threshold")):
    query = f"""
    SELECT expense_description
    FROM expense
    GROUP BY expense_description
    HAVING AVG(cost) > ?
    """
    cursor.execute(query, (cost,))
    result = cursor.fetchall()
    return {"expense_descriptions": [row[0] for row in result]}

# Endpoint to get members with a specific t-shirt size
@app.get("/v1/bird/student_club/members_by_tshirt_size", summary="Get members with a specific t-shirt size")
async def get_members_by_tshirt_size(t_shirt_size: str = Query(..., description="T-shirt size")):
    query = f"""
    SELECT first_name, last_name
    FROM member
    WHERE t_shirt_size = ?
    """
    cursor.execute(query, (t_shirt_size,))
    result = cursor.fetchall()
    return {"members": [{"first_name": row[0], "last_name": row[1]} for row in result]}

# Endpoint to get percentage of zip codes with a specific type
@app.get("/v1/bird/student_club/zip_code_percentage", summary="Get percentage of zip codes with a specific type")
async def get_zip_code_percentage(type: str = Query(..., description="Type of zip code")):
    query = f"""
    SELECT CAST(SUM(CASE WHEN type = ? THEN 1 ELSE 0 END) AS REAL) * 100 / COUNT(zip_code)
    FROM zip_code
    """
    cursor.execute(query, (type,))
    result = cursor.fetchone()
    return {"percentage": result[0]}

# Endpoint to get distinct event names and locations with remaining budget greater than 0
@app.get("/v1/bird/student_club/events_with_remaining_budget", summary="Get distinct event names and locations with remaining budget greater than 0")
async def get_events_with_remaining_budget():
    query = f"""
    SELECT DISTINCT T1.event_name, T1.location
    FROM event AS T1
    INNER JOIN budget AS T2 ON T1.event_id = T2.link_to_event
    WHERE T2.remaining > 0
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return {"events": [{"event_name": row[0], "location": row[1]} for row in result]}

# Endpoint to get event details with specific expense description and cost range
@app.get("/v1/bird/student_club/event_details_by_expense", summary="Get event details with specific expense description and cost range")
async def get_event_details_by_expense(expense_description: str = Query(..., description="Expense description"), min_cost: float = Query(..., description="Minimum cost"), max_cost: float = Query(..., description="Maximum cost")):
    query = f"""
    SELECT T1.event_name, T1.event_date
    FROM event AS T1
    INNER JOIN budget AS T2 ON T1.event_id = T2.link_to_event
    INNER JOIN expense AS T3 ON T2.budget_id = T3.link_to_budget
    WHERE T3.expense_description = ? AND T3.cost > ? AND T3.cost < ?
    """
    cursor.execute(query, (expense_description, min_cost, max_cost))
    result = cursor.fetchall()
    return {"event_details": [{"event_name": row[0], "event_date": row[1]} for row in result]}

# Endpoint to get distinct members with expenses greater than a specific value
@app.get("/v1/bird/student_club/members_with_expenses", summary="Get distinct members with expenses greater than a specific value")
async def get_members_with_expenses(cost: float = Query(..., description="Cost threshold")):
    query = f"""
    SELECT DISTINCT T1.first_name, T1.last_name, T2.major_name
    FROM member AS T1
    INNER JOIN major AS T2 ON T2.major_id = T1.link_to_major
    INNER JOIN expense AS T3 ON T1.member_id = T3.link_to_member
    WHERE T3.cost > ?
    """
    cursor.execute(query, (cost,))
    result = cursor.fetchall()
    return {"members": [{"first_name": row[0], "last_name": row[1], "major_name": row[2]} for row in result]}

# Endpoint to get distinct cities and counties with income greater than a specific value
@app.get("/v1/bird/student_club/cities_counties_by_income", summary="Get distinct cities and counties with income greater than a specific value")
async def get_cities_counties_by_income(amount: float = Query(..., description="Income threshold")):
    query = f"""
    SELECT DISTINCT T3.city, T3.county
    FROM income AS T1
    INNER JOIN member AS T2 ON T1.link_to_member = T2.member_id
    INNER JOIN zip_code AS T3 ON T3.zip_code = T2.zip
    WHERE T1.amount > ?
    """
    cursor.execute(query, (amount,))
    result = cursor.fetchall()
    return {"cities_counties": [{"city": row[0], "county": row[1]} for row in result]}

# Endpoint to get member IDs with multiple events and sorted by total cost
@app.get("/v1/bird/student_club/member_ids_with_multiple_events", summary="Get member IDs with multiple events and sorted by total cost")
async def get_member_ids_with_multiple_events():
    query = f"""
    SELECT T2.member_id
    FROM expense AS T1
    INNER JOIN member AS T2 ON T1.link_to_member = T2.member_id
    INNER JOIN budget AS T3 ON T1.link_to_budget = T3.budget_id
    INNER JOIN event AS T4 ON T3.link_to_event = T4.event_id
    GROUP BY T2.member_id
    HAVING COUNT(DISTINCT T4.event_id) > 1
    ORDER BY SUM(T1.cost) DESC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"member_id": result[0]}

# Endpoint to get average cost of expenses for members not in 'Member' position
@app.get("/v1/bird/student_club/average_cost", summary="Get average cost of expenses for members not in 'Member' position")
async def get_average_cost(position: str = Query(..., description="Position of the member")):
    query = f"""
    SELECT AVG(T1.cost) FROM expense AS T1
    INNER JOIN member as T2 ON T1.link_to_member = T2.member_id
    WHERE T2.position != ?
    """
    cursor.execute(query, (position,))
    result = cursor.fetchone()
    return {"average_cost": result[0]}

# Endpoint to get event names with parking expenses below average cost
@app.get("/v1/bird/student_club/events_with_parking", summary="Get event names with parking expenses below average cost")
async def get_events_with_parking(category: str = Query(..., description="Category of the budget")):
    query = f"""
    SELECT T1.event_name FROM event AS T1
    INNER JOIN budget AS T2 ON T1.event_id = T2.link_to_event
    INNER JOIN expense AS T3 ON T2.budget_id = T3.link_to_budget
    WHERE T2.category = ? AND T3.cost < (SELECT AVG(cost) FROM expense)
    """
    cursor.execute(query, (category,))
    result = cursor.fetchall()
    return {"event_names": [row[0] for row in result]}

# Endpoint to get percentage of meeting expenses
@app.get("/v1/bird/student_club/meeting_expense_percentage", summary="Get percentage of meeting expenses")
async def get_meeting_expense_percentage():
    query = f"""
    SELECT SUM(CASE WHEN T1.type = 'Meeting' THEN T3.cost ELSE 0 END) * 100 / SUM(T3.cost)
    FROM event AS T1
    INNER JOIN budget AS T2 ON T1.event_id = T2.link_to_event
    INNER JOIN expense AS T3 ON T2.budget_id = T3.link_to_budget
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"meeting_expense_percentage": result[0]}

# Endpoint to get budget ID for a specific expense description
@app.get("/v1/bird/student_club/budget_id_for_expense", summary="Get budget ID for a specific expense description")
async def get_budget_id_for_expense(expense_description: str = Query(..., description="Description of the expense")):
    query = f"""
    SELECT T2.budget_id FROM expense AS T1
    INNER JOIN budget AS T2 ON T1.link_to_budget = T2.budget_id
    WHERE T1.expense_description = ?
    ORDER BY T1.cost DESC LIMIT 1
    """
    cursor.execute(query, (expense_description,))
    result = cursor.fetchone()
    return {"budget_id": result[0]}

# Endpoint to get top 5 members by spent budget
@app.get("/v1/bird/student_club/top_members_by_spent", summary="Get top 5 members by spent budget")
async def get_top_members_by_spent():
    query = f"""
    SELECT T3.first_name, T3.last_name FROM expense AS T1
    INNER JOIN budget AS T2 ON T1.link_to_budget = T2.budget_id
    INNER JOIN member AS T3 ON T1.link_to_member = T3.member_id
    ORDER BY T2.spent DESC LIMIT 5
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return {"top_members": [{"first_name": row[0], "last_name": row[1]} for row in result]}

# Endpoint to get members with expenses above average cost
@app.get("/v1/bird/student_club/members_above_average_cost", summary="Get members with expenses above average cost")
async def get_members_above_average_cost():
    query = f"""
    SELECT DISTINCT T3.first_name, T3.last_name, T3.phone FROM expense AS T1
    INNER JOIN budget AS T2 ON T1.link_to_budget = T2.budget_id
    INNER JOIN member AS T3 ON T3.member_id = T1.link_to_member
    WHERE T1.cost > (
        SELECT AVG(T1.cost) FROM expense AS T1
        INNER JOIN budget AS T2 ON T1.link_to_budget = T2.budget_id
        INNER JOIN member AS T3 ON T3.member_id = T1.link_to_member
    )
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return {"members": [{"first_name": row[0], "last_name": row[1], "phone": row[2]} for row in result]}

# Endpoint to get percentage difference between members from New Jersey and Vermont
@app.get("/v1/bird/student_club/percentage_difference", summary="Get percentage difference between members from New Jersey and Vermont")
async def get_percentage_difference():
    query = f"""
    SELECT CAST((SUM(CASE WHEN T2.state = 'New Jersey' THEN 1 ELSE 0 END) - SUM(CASE WHEN T2.state = 'Vermont' THEN 1 ELSE 0 END)) AS REAL) * 100 / COUNT(T1.member_id) AS diff
    FROM member AS T1
    INNER JOIN zip_code AS T2 ON T2.zip_code = T1.zip
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"percentage_difference": result[0]}

# Endpoint to get major details for a specific member
@app.get("/v1/bird/student_club/major_details", summary="Get major details for a specific member")
async def get_major_details(first_name: str = Query(..., description="First name of the member"), last_name: str = Query(..., description="Last name of the member")):
    query = f"""
    SELECT T2.major_name, T2.department FROM member AS T1
    INNER JOIN major AS T2 ON T2.major_id = T1.link_to_major
    WHERE T1.first_name = ? AND T1.last_name = ?
    """
    cursor.execute(query, (first_name, last_name))
    result = cursor.fetchone()
    return {"major_name": result[0], "department": result[1]}

# Endpoint to get expense details for a specific description
@app.get("/v1/bird/student_club/expense_details", summary="Get expense details for a specific description")
async def get_expense_details(expense_description: str = Query(..., description="Description of the expense")):
    query = f"""
    SELECT T2.first_name, T2.last_name, T1.cost FROM expense AS T1
    INNER JOIN member AS T2 ON T1.link_to_member = T2.member_id
    WHERE T1.expense_description = ?
    """
    cursor.execute(query, (expense_description,))
    result = cursor.fetchall()
    return {"expense_details": [{"first_name": row[0], "last_name": row[1], "cost": row[2]} for row in result]}

# Endpoint to get member details for a specific major
@app.get("/v1/bird/student_club/member_details_by_major", summary="Get member details for a specific major")
async def get_member_details_by_major(major_name: str = Query(..., description="Name of the major")):
    query = f"""
    SELECT T1.last_name, T1.phone FROM member AS T1
    INNER JOIN major AS T2 ON T2.major_id = T1.link_to_major
    WHERE T2.major_name = ?
    """
    cursor.execute(query, (major_name,))
    result = cursor.fetchall()
    return {"member_details": [{"last_name": row[0], "phone": row[1]} for row in result]}

# Endpoint to get budget details for a specific event
@app.get("/v1/bird/student_club/budget_details_by_event", summary="Get budget details for a specific event")
async def get_budget_details_by_event(event_name: str = Query(..., description="Name of the event")):
    query = f"""
    SELECT T2.category, T2.amount FROM event AS T1
    INNER JOIN budget AS T2 ON T1.event_id = T2.link_to_event
    WHERE T1.event_name = ?
    """
    cursor.execute(query, (event_name,))
    result = cursor.fetchall()
    return {"budget_details": [{"category": row[0], "amount": row[1]} for row in result]}

# Endpoint to get event names with food budget
@app.get("/v1/bird/student_club/events_with_food_budget", summary="Get event names with food budget")
async def get_events_with_food_budget(category: str = Query(..., description="Category of the budget")):
    query = f"""
    SELECT T1.event_name FROM event AS T1
    INNER JOIN budget AS T2 ON T1.event_id = T2.link_to_event
    WHERE T2.category = ?
    """
    cursor.execute(query, (category,))
    result = cursor.fetchall()
    return {"event_names": [row[0] for row in result]}

# Endpoint to get member income details for a specific date
@app.get("/v1/bird/student_club/member_income_by_date", summary="Get member income details for a specific date")
async def get_member_income_by_date(date_received: str = Query(..., description="Date received")):
    query = f"""
    SELECT DISTINCT T3.first_name, T3.last_name, T4.amount FROM event AS T1
    INNER JOIN attendance AS T2 ON T1.event_id = T2.link_to_event
    INNER JOIN member AS T3 ON T3.member_id = T2.link_to_member
    INNER JOIN income AS T4 ON T4.link_to_member = T3.member_id
    WHERE T4.date_received = ?
    """
    cursor.execute(query, (date_received,))
    result = cursor.fetchall()
    return {"member_income": [{"first_name": row[0], "last_name": row[1], "amount": row[2]} for row in result]}

# Endpoint to get distinct expense categories for a specific description
@app.get("/v1/bird/student_club/expense_categories", summary="Get distinct expense categories for a specific description")
async def get_expense_categories(expense_description: str = Query(..., description="Description of the expense")):
    query = f"""
    SELECT DISTINCT T2.category FROM expense AS T1
    INNER JOIN budget AS T2 ON T1.link_to_budget = T2.budget_id
    WHERE T1.expense_description = ?
    """
    cursor.execute(query, (expense_description,))
    result = cursor.fetchall()
    return {"expense_categories": [row[0] for row in result]}

# Endpoint to get member details for a specific position
@app.get("/v1/bird/student_club/member_details_by_position", summary="Get member details for a specific position")
async def get_member_details_by_position(position: str = Query(..., description="Position of the member")):
    query = f"""
    SELECT T1.first_name, T1.last_name, college FROM member AS T1
    INNER JOIN major AS T2 ON T2.major_id = T1.link_to_major
    WHERE T1.position = ?
    """
    cursor.execute(query, (position,))
    result = cursor.fetchall()
    return {"member_details": [{"first_name": row[0], "last_name": row[1], "college": row[2]} for row in result]}

# Endpoint to get total spent and event name for a specific category
@app.get("/v1/bird/student_club/total_spent_by_category", summary="Get total spent and event name for a specific category")
async def get_total_spent_by_category(category: str = Query(..., description="Category of the budget")):
    query = f"""
    SELECT SUM(T1.spent), T2.event_name FROM budget AS T1
    INNER JOIN event AS T2 ON T1.link_to_event = T2.event_id
    WHERE T1.category = ?
    GROUP BY T2.event_name
    """
    cursor.execute(query, (category,))
    result = cursor.fetchall()
    return {"total_spent": [{"total_spent": row[0], "event_name": row[1]} for row in result]}

# Endpoint to get city for a specific member
@app.get("/v1/bird/student_club/city_for_member", summary="Get city for a specific member")
async def get_city_for_member(first_name: str = Query(..., description="First name of the member"), last_name: str = Query(..., description="Last name of the member")):
    query = f"""
    SELECT T2.city FROM member AS T1
    INNER JOIN zip_code AS T2 ON T2.zip_code = T1.zip
    WHERE T1.first_name = ? AND T1.last_name = ?
    """
    cursor.execute(query, (first_name, last_name))
    result = cursor.fetchone()
    return {"city": result[0]}

# Endpoint to get member details for a specific city, state, and zip code
@app.get("/v1/bird/student_club/member_details_by_location", summary="Get member details for a specific city, state, and zip code")
async def get_member_details_by_location(city: str = Query(..., description="City of the member"), state: str = Query(..., description="State of the member"), zip_code: int = Query(..., description="Zip code of the member")):
    query = f"""
    SELECT T1.first_name, T1.last_name, T1.position FROM member AS T1
    INNER JOIN zip_code AS T2 ON T2.zip_code = T1.zip
    WHERE T2.city = ? AND T2.state = ? AND T2.zip_code = ?
    """
    cursor.execute(query, (city, state, zip_code))
    result = cursor.fetchall()
    return {"member_details": [{"first_name": row[0], "last_name": row[1], "position": row[2]} for row in result]}
