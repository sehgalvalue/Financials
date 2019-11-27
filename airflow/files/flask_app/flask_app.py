from flask import Flask, make_response, request, render_template
import pandas as pd
import glob
from datetime import datetime as dt
from flask_heroku import Heroku
import os
import psycopg2
import subprocess
from sqlalchemy import create_engine
import sqlalchemy

app = Flask(__name__)
heroku = Heroku(app)

@app.route('/')
def form():
    db_url = app.config['SQLALCHEMY_DATABASE_URI']
    engine = create_engine(db_url)
    #df_options = pd.read_csv('/home/lscanlon/airflow/files/flask_app/options_flask_app.csv')
    df_options = pd.read_sql(sql=sqlalchemy.text('select * from options'), con = engine)
    regions = sorted(list(set(df_options['Regions'].dropna())))
    industries = sorted(list(set(df_options['Industry'].dropna())))
    siccodes = sorted(list(set(df_options['SIC_codes'].dropna())))
    statuses = sorted(list(set(df_options['Status'].dropna())))
    leps = sorted(list(set(df_options['LEP'].dropna())))
    return render_template('template.html', regions=regions, industries=industries, siccodes=siccodes, statuses=statuses, leps=leps)

@app.route('/merge', methods=["POST","GET"])
def transform_view():
    try:
        file = request.files["data_file"]
    except Exception:
        message = 'No file'
        region = request.form.getlist("regions")
        industry = request.form.getlist("industries")
        siccode = request.form.getlist("siccodes")
        status = request.form.getlist("statuses")
        leps = request.form.getlist("LEP")
        if all([region == [], industry == [], siccode == [], status == [], leps == []]):
            message = "You haven't uploaded a file or selected any filters, please go back and upload a file or select filters" 
        else:
            db_url = app.config['SQLALCHEMY_DATABASE_URI']
            engine = create_engine(db_url)
            check = engine.has_table('criteria_data')
            if check == True:
                message = "There is an error, please wait 10 minutes and then try uploading your file again"
            else:
                message = "You selected; "
                criteria = pd.DataFrame()
                n = max([len(region), len(industry), len(siccode), len(status), len(leps)])
                for x in [['Region', region], ['Industry', industry], ['SIC Code', siccode], ['CompanyStatus', status], ['LEP', leps]]:
                    if len(x[1]) > 0:
                        message = message + "<br>" + str(x[0]) + ": " + str(', '.join(x[1]))
                        if len(x[1]) < n:
                            x[1] = x[1] + [x[1][0]]*(n-len(x[1]))
                        if x[0] == 'SIC Code':
                            criteria['SICCode_SicText_1'] = x[1]
                            criteria['SICCode_SicText_2'] = x[1]
                            criteria['SICCode_SicText_3'] = x[1]
                            criteria['SICCode_SicText_4'] = x[1]
                        else:
                            criteria[x[0]] = x[1]
                message = message + "<br><br> You will receive an email shortly with information about companies meeting the criteria you have selected"
            #files_criteria = glob.glob('/home/lscanlon/airflow/files/input_files/criteria_file*.csv')
            #n = len(files_criteria)
            #if n == 0:
            #    criteria.to_csv('/home/lscanlon/airflow/files/input_files/criteria_file.csv', index=False)
            #else:
            #    criteria.to_csv('/home/lscanlon/airflow/files/input_files/criteria_file'+str(n)+'.csv', index=False)
                criteria.to_sql('criteria_data', engine, if_exists='fail')
                email = request.form['text_email_criteria']
                ref = request.form['text_reference_criteria']
                df1 = pd.DataFrame({'Email':email, 'Reference':ref}, index=[0])
                df1.to_sql('criteria_email', engine, if_exists='fail')
            #files_email = glob.glob('/home/lscanlon/airflow/files/input_files/email_address_criteria*.csv')
            #n = len(files_email)
            #if n == 0:
            #    df1.to_csv('/home/lscanlon/airflow/files/input_files/email_address_criteria.csv', index=False)
            #else:
            #    df1.to_csv('/home/lscanlon/airflow/files/input_files/email_address_criteria'+str(n)+'.csv', index=False)
    else:
        df = pd.read_csv(file, encoding = 'latin1')
        db_url = app.config['SQLALCHEMY_DATABASE_URI']
        engine = create_engine(db_url)
        check = engine.has_table('input_data')
        if check == True:
            message = "There is an error, please wait 10 minutes and then try uploading your file again"
        else:
            message = "File received, you will receive an email shortly with information about the companies in your file"
            df.to_sql('input_data', engine, if_exists='fail')
        #files_input = glob.glob('/home/lscanlon/airflow/files/input_files/input_file*.csv')
        #n = len(files_input)
        #if n == 0:
        #    df.to_csv('/home/lscanlon/airflow/files/input_files/input_file.csv', index=False)
        #else:
        #    df.to_csv('/home/lscanlon/airflow/files/input_files/input_file'+str(n)+'.csv', index=False)
            email = request.form['text_email_file']
            ref = request.form['text_reference_file']
            df1 = pd.DataFrame({'Email':email, 'Reference':ref}, index=[0])
            df1.to_sql('input_email', engine, if_exists='fail')
        #files_email = glob.glob('/home/lscanlon/airflow/files/input_files/email_address_file*.csv')
        #n = len(files_email)
        #if n == 0:
        #    df1.to_csv('/home/lscanlon/airflow/files/input_files/email_address_file.csv', index=False)
        #else:
        #    df1.to_csv('/home/lscanlon/airflow/files/input_files/email_address_file'+str(n)+'.csv', index=False)
        #message = "File received, you will receive an email shortly with information about the companies in your file"
    return message


@app.errorhandler(404)
def page_not_found(e):
    # note that we set the 404 status explicitly
    return render_template('404.html'), 404

@app.errorhandler(400)
def page_not_found(e):
    # note that we set the 400 status explicitly
    return render_template('400.html'), 400

if __name__ == "__main__":
    app.run(debug=True)
