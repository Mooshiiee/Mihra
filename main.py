import subprocess
import psycopg2
from io import StringIO


def connectionToDB():
    conn = None
    try:
        print('connectinng to database')
        conn = psycopg2.connect("dbname=test user=postgres password=admin host=localhost")
        print('db conneciton sucessful')

        #create cursor
        cursor = conn.cursor()
        cursor.execute('SELECT version()')
        version = cursor.fetchone()
        print(version)

    except(Exception, psycopg2.DatabaseError) as error:
        print('db connection failed')
        print(error)
    finally:
        if conn is not None:
            print('sending off to processing')
            return conn

def csvImport(conn):
    #create cursor
    cursor = conn.cursor()

    #define columns as dictionary
    columns = [
    ("Year", "INTEGER"),
    ("Industry_aggregation_NZSIOC", "VARCHAR(12)"),
    ("Industry_code_NZSIOC", "VARCHAR(12)"),
    ("Industry_name_NZSIOC", "VARCHAR(255)"),
    ("Units", "VARCHAR(255)"),
    ("Variable_code", "VARCHAR(255)"),
    ("Variable_name", "VARCHAR(255)"),
    ("Variable_category", "VARCHAR(255)"),
    ("Value", "VARCHAR(255)"),
    ("Industry_code_ANZSIC06", "VARCHAR(255)"),
    ]
    
    create_table_command = 'CREATE TABLE IF NOT EXISTS testtable ('

    #loop dictionary 
    for column, datatype in columns:
        create_table_command += f"{column} {datatype}, "
    #add ending parenthesis 
    create_table_command = create_table_command.rstrip(", ") + ")"

    print('CREATE TABLE command created')
    print(create_table_command)

    #drop existing table if it exists
    cursor.execute('DROP TABLE IF EXISTS testtable')
    print('dropping testtable if it exists')

    #execute create table command
    cursor.execute(create_table_command)
    print('CREATE TABLE command executed')


    #now that table is created, we can do \copy

    with open('C:\\Users\\Mustafa\\Documents\\Mihra\\annualenterprise2021.csv', 'r') as f:
        # Skip the header row
        next(f)
        # Create a file-like object using StringIO
        file_like = StringIO(f.read())
    print('created StringIO representation')

    cursor.copy_from(file_like,'testtable',sep=',',columns=(
        'year', 
        'industry_aggregation_nzsioc', 
        'industry_code_nzsioc', 
        'industry_name_nzsioc', 
        'units', 
        'variable_code', 
        'variable_name', 
        'variable_category', 
        'value', 
        'industry_code_anzsic06'
        ))

    print('copy executed')
    conn.commit()

if __name__ == "__main__":
    conn = connectionToDB()
    csvImport(conn)






 