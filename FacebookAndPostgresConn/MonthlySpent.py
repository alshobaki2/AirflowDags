from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators import PythonOperator
from airflow.hooks import PostgresHook
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from datetime import datetime, timedelta
# facebook lib
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.api import FacebookAdsApi



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
 
}

dag = DAG('monthly_spent', default_args=default_args, schedule_interval='0 0 1 * *')



t1 = BashOperator(
    task_id='Jobstartdate',
    bash_command='date ',
    dag=dag)

t3 = BashOperator(
    task_id='JobEnddate',
    bash_command='date ',
    dag=dag)



def monthly_spent(TaskName , ds, prev_ds, **kwargs  ):
    facebook = Variable.get("facebook_auth", deserialize_json=True)
    access_token =  facebook["access_token"]
    ad_account_id =  facebook["ad_account_id"]  
    app_secret = facebook["app_secret"]  
    app_id = facebook["app_id"]   
    FacebookAdsApi.init(access_token=access_token)
    fields = [ 'spend', 'account_id', ]
    fromDate = prev_ds
    toDate = (datetime.strptime(ds, '%Y-%m-%d') + timedelta(days=-1)).strftime('%Y-%m-%d') 
    params = { 'level': 'account', 'time_range': {'since': fromDate ,'until': toDate },}
    spendrows = AdAccount(ad_account_id).get_insights(    fields=fields,    params=params,)

    total = 0.0 
    for row in spendrows:
        total = total + float ( row["spend"])
    row = (total, ds)

    connDB =  PostgresHook (postgres_conn_id = 'airflow')
    FB_json_conn = BaseHook.get_connection('FB_json_conn').extra_dejson
    scommand = "insert into monthly_cost values( %s ,%s) "
    connDB.run(scommand, parameters =row)
    print ( "done, Cost from "+ prev_ds+ " to " + ds )




t2 = PythonOperator(
    task_id='monthly_spent',
    provide_context=True,
    python_callable=monthly_spent,
    op_kwargs={'TaskName': 'monthly_spent'},
    dag=dag,
)


t1 >> t2 >> t3

