import sqlite3


def load_metrics_tables():

    database_name = "career_professionals_data.db"

    connection = sqlite3.connect(database_name)
    cursor = connection.cursor()

    # Create a Facts Table using the SQL Dimension tables already created 

    # UPSKILLING FACTS TABLE

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS UPSKILLING AS 
        SELECT 
            P.professional_id,
            P.years_experience,
            COUNT(DISTINCT S.skill_id) AS NUM_SKILLS,
            COUNT(DISTINCT C.certification_id) AS NUM_CERTIFICATIONS,
            SUM(S.years_experience) AS SKILL_YEARS_EXP
        FROM PROFESSIONALS P
        LEFT JOIN SKILLS S ON P.professional_id = S.professional_id
        LEFT JOIN CERTIFICATIONS C ON P.professional_id = C.professional_id
        GROUP BY P.professional_id, P.years_experience
        ORDER BY P.professional_id
    ''')

    # CAREER/ JOB SWITCHING FACTS TABLE
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS JOB_SWITCH AS 
    SELECT
        J1.professional_id,
        J1.job_id AS previous_job_id,
        J1.role AS previous_role,
        J1.company AS previous_company,
        J1.industry AS previous_industry,
        J1.end_date AS previous_end_date,
        J2.job_id AS next_job_id,
        J2.role AS next_role,
        J2.company AS next_company,
        J2.industry AS next_industry,
        J2.start_date AS next_start_date,
        J2.salary_band AS next_salary_band
        FROM
            jobs J1
        JOIN
            jobs J2 ON J1.professional_id = J2.professional_id
        WHERE
            J1.end_date < J2.start_date
        ORDER BY
            J1.professional_id, J1.end_date
        
    ''')



    connection.commit()
    connection.close()