import json
import pandas as pd
import sqlite3


def etl_career_analytics():

    # EXTRACT & TRANSFORM

    # load the input JSON data
    with open("professionals_nested.json", "r") as f:
        data = json.load(f)

    # print(data['professionals'][0])

    all_professionals_data = data['professionals']

    # Initialize separate data structures for representing nested data as dimensional models
    # List of Dicts for Professional Data
    dim_professional_data = []

    # DFs for all others
    dim_job_df = pd.DataFrame()
    dim_skill_df = pd.DataFrame()
    dim_certification_df = pd.DataFrame()
    dim_education_df = pd.DataFrame()


    # Run a loop to process all professional individuals' data
    for professional in all_professionals_data:

        # Assign professional_id to use across all tables
        professional_id = professional["professional_id"]

        # Append Individual Professional Data
        dim_professional_data.append({
            "professional_id" : professional_id,
            "years_experience" : professional["years_experience"],
            "current_industry" : professional["current_industry"],
            "current_role" : professional["current_role"],
            "education_level" : professional["education_level"]
        })

        # Append Job Data
        job_temp = pd.json_normalize(professional, record_path='jobs')
        job_temp['professional_id'] = professional_id
        dim_job_df = pd.concat([dim_job_df, job_temp], ignore_index=True)

        # Append Skill Data
        skill_temp = pd.json_normalize(professional, record_path='skills')
        skill_temp['professional_id'] = professional_id
        dim_skill_df = pd.concat([dim_skill_df, skill_temp], ignore_index=True)

        # Append Certification Data
        certification_temp = pd.json_normalize(professional, record_path='certifications')
        certification_temp['professional_id'] = professional_id
        dim_certification_df = pd.concat([dim_certification_df, certification_temp], ignore_index=True)

        # Append Education Data
        education_temp = pd.json_normalize(professional, record_path='education')
        education_temp['professional_id'] = professional_id
        dim_education_df = pd.concat([dim_education_df, education_temp], ignore_index=True)


    # print(pd.DataFrame(professional_data[:5]))

    # Create DataFrames for professional_df datasets created
    dim_professional_df = pd.DataFrame(data=dim_professional_data)

    # Data Quality checks and Validation

    # Outliers - Years of experience over 50 years

    def yoe_outliers(dataframe: pd.DataFrame, columns:list, check_num:int) -> None:
        for col in columns:
            outliers = dataframe[dataframe[col] > check_num]   # Catch individuals with over 50 years of Work Experience 
            if len(outliers)>0:
                print(f"Outlier found in column: {col} at dataframe indices: {list(outliers.index)}")
            else:
                print(f"Outliers check for column: {col} Successful!")


    print("\nChecking Years Experience for Individual Professional Data...")
    yoe_outliers(
        dataframe= dim_professional_df,
        columns= ['years_experience'],
        check_num=50
    )       
    print("\nChecking Years Experience for Skills Data...")
    yoe_outliers(
        dataframe= dim_skill_df,
        columns= ['years_experience'],
        check_num=50
    )

    # Date Values Format and nulls

    def date_validation(dataframe: pd.DataFrame, date_columns:list):
        for col in date_columns:
            dataframe[col] = pd.to_datetime(dataframe[col], errors="coerce")  # Erroneous dates will be forced to NaT datatype
            null_dates = dataframe[dataframe[col].isna()]
            # print(null_dates)
            if len(null_dates)>0:
                print(f"Null Dates found for column: {col} at dataframe indices: {list(null_dates.index)}")
            else:
                print(f"Date Validation for column: {col} Successful!")
        return dataframe

    print("\nChecking for Jobs Data...")
    dim_job_df = date_validation(
        dataframe=dim_job_df, 
        date_columns=['start_date', 'end_date']
        )
    print("\nChecking for Certification Data...")
    dim_certification_df = date_validation(
        dataframe=dim_certification_df,
        date_columns= ['date_earned', 'expiration_date']
        )
    print("\nChecking for Education Data...")
    dim_education_df = date_validation(
        dataframe= dim_education_df,
        date_columns= ['graduation_date']
    )

    # # Save DFs as CSVs (optional)

    # csv_path = "data_CSVs/"
    # if not os.path.exists(csv_path):
    #     os.makedirs(csv_path)

    # dim_professional_df.to_csv("data_CSVs/professionals.csv", index=False)
    # dim_job_df.to_csv("data_CSVs/jobs.csv", index=False)
    # dim_skill_df.to_csv("data_CSVs/skills.csv", index=False)
    # dim_certification_df.to_csv("data_CSVs/certifications.csv", index=False)
    # dim_education_df.to_csv("data_CSVs/educations.csv", index=False)


    # LOAD

    # Create a dimensional model usng SQLite3

    # Assign a database name
    database_name = "career_professionals_data.db"

    connection = sqlite3.connect(database_name)
    cursor = connection.cursor()


    # Load Dataframes as Dimension Tables into the SQL Database
    dim_professional_df.to_sql("PROFESSIONALS", connection, if_exists='append', index=False)
    dim_skill_df.to_sql("SKILLS", connection, if_exists='append', index=False)
    dim_job_df.to_sql("JOBS", connection, if_exists='append', index=False)
    dim_certification_df.to_sql("CERTIFICATIONS", connection, if_exists='append', index=False)
    dim_education_df.to_sql("EDUCATIONS", connection, if_exists='append', index=False)


    connection.commit()
    connection.close()






