import table_schemas

import sys
import csv
import io
import logging
import requests
import base64
import json
from datetime import datetime, timedelta
from dateutil.parser import parse

import functions_framework
from google.cloud import bigquery, secretmanager_v1, logging as cloud_logging

#Slack push notification metadata
SLACK_OAUTH = 'xoxb-4075992379237-7396238255316-LGfVI8jdrrE0Y0T18CLtnj57'
SLACK_NOTIFICATION_URL = 'https://slack.com/api/chat.postMessage'
SLACK_NOTIFICATION_CHANNEL_ID = 'C07C8GQ51BJ'

def send_slack_notification(message):
    headers = {
        'Content-type' : 'application/json',
        'Authorization': f"Bearer {SLACK_OAUTH}"
    }
    payload = {
        'text': message,
        'channel': SLACK_NOTIFICATION_CHANNEL_ID
    }
    response = requests.post(SLACK_NOTIFICATION_URL, json = payload, headers = headers)
    if response.status_code == 200:
        logging.info('Error happened, Notification is sent to Slack!!!')
    else:
        logger.error('Slack notification failed with status code {}.\nReason: {}.\nHeaders: {}.\nContent:{}'.format(
                    response.status_code,
                    response.reason,
                    response.headers,
                    response.content,
                ))
        



# Set up logging
log_file = 'appsflyer_api.log'
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s',
                    handlers=[
                        logging.FileHandler(log_file, encoding ='utf-8'),
                        logging.StreamHandler(sys.stdout)
                    ])
logger = logging.getLogger()



project_name = 'maximizer-omega'
project_id = '116248898364'
#this is GCP's project ID for Cloud Function
staging_dataset = 'appsflyer_staging'

BATCH_SIZE = 30000
LIMIT_NUM_REQUEST = 200000


cloud_logging.Client(project=project_id).setup_logging()

logger = logging.getLogger()

# Flag for local debugging
DEBUG = False


products = {
    'tales_of_crown': {
        'app_ids': [
            {'platform': 'android', 'app_id': 'com.superrabbitgames.crownquest'},
            {'platform': 'ios', 'app_id': 'id6471469993'},
        ],
        'dataset': 'all_ToC_appsflyer',
        'secret_manager_api_key_id': 'appsflyer_ToC_api_token',
    },
    'cats_and_soup': {
        'app_ids': [
            {'platform': 'android', 'app_id': 'com.hidea.cat'},
            {'platform': 'ios', 'app_id': 'id1581431235'}
        ],
        'dataset': 'all_catsnsoup_appsflyer',
        'secret_manager_api_key_id': 'appsflyer_catsnsoup_api_token',
    },
}

partitioning_type = {
    'real_time': 'HOUR',
    'continuous': 'HOUR',
    'daily': 'DAY',
}

# The following reports result in an error for now and require the client to enable them: 
#   reinstalls, reinstalls_organic, detection, blocked_in_app_events_report, fraud-post-inapps, blocked_clicks_report
#   More info: https://support.appsflyer.com/hc/en-us/articles/10164066030737-Reinstalls#reinstalls-raw-data-reports
# The other commented reports are technically available in the API but they never have any
#   data. Need to confirm schema before enabling them.
# Data freshness: https://support.appsflyer.com/hc/en-us/articles/360000310629-Data-freshness-and-time-zone-support#data-freshness-types
reports = [
    {
        'report_name': 'installs_report', 'data_freshness': 'real_time', 'type': 'raw',
        'partitioning_field': 'Event_Time', 'schema': table_schemas.SCHEMA_IN_APP_EVENTS_REPORT,
    },
    {
        'report_name': 'in_app_events_report', 'data_freshness': 'real_time', 'type': 'raw',
        'partitioning_field': 'Event_Time', 'schema': table_schemas.SCHEMA_IN_APP_EVENTS_REPORT,
    },
    # Schema might be wrong
    # {
    #     'report_name': 'uninstall_events_report', 'data_freshness': 'daily', 'type': 'raw',
    #     'partitioning_field': 'Event_Time', 
    #     # 'schema': table_schemas.SCHEMA_UNINSTALL_EVENTS_REPORT,
    # },
    # Schema might be wrong
    {
        'report_name': 'reinstalls', 'data_freshness': 'real_time', 'type': 'raw',
        'partitioning_field': 'Event_Time', 'schema': table_schemas.SCHEMA_IN_APP_EVENTS_REPORT,
    },
    # Schema might be wrong
    {
        'report_name': 'organic_installs_report', 'data_freshness': 'continuous', 'type': 'raw', 
        'partitioning_field': 'Event_Time', 'schema': table_schemas.SCHEMA_IN_APP_EVENTS_REPORT,
    },
    # Schema might be wrong
    {
        'report_name': 'organic_in_app_events_report', 'data_freshness': 'continuous', 'type': 'raw',
        'partitioning_field': 'Event_Time', 'schema': table_schemas.SCHEMA_IN_APP_EVENTS_REPORT,
    },
    # Schema might be wrong
    # {
    #     'report_name': 'organic_uninstall_events_report', 'data_freshness': 'daily', 'type': 'raw',
    #     'partitioning_field': 'Event_Time', 
    #     # 'schema': table_schemas.SCHEMA_UNINSTALL_EVENTS_REPORT,
    # },
    # Schema might be wrong
    {
        'report_name': 'reinstalls_organic', 'data_freshness': 'daily', 'type': 'raw',
        'partitioning_field': 'Event_Time', 'schema': table_schemas.SCHEMA_IN_APP_EVENTS_REPORT,
    },
    # Schema might be wrong
    {
        'report_name': 'installs-retarget', 'data_freshness': 'real_time', 'type': 'raw',
        'partitioning_field': 'Event_Time', 'schema': table_schemas.SCHEMA_IN_APP_EVENTS_REPORT,
    },
    {
        'report_name': 'in-app-events-retarget', 'data_freshness': 'real_time', 'type': 'raw',
        'partitioning_field': 'Event_Time', 'schema': table_schemas.SCHEMA_IN_APP_EVENTS_REPORT,
    },
    {
        'report_name': 'ad_revenue_raw', 'data_freshness': 'daily', 'type': 'raw',
        'partitioning_field': 'Event_Time', 'schema': table_schemas.SCHEMA_IN_APP_EVENTS_REPORT,
    },
    {
        'report_name': 'ad_revenue_organic_raw', 'data_freshness': 'daily', 'type': 'raw',
        'partitioning_field': 'Event_Time', 'schema': table_schemas.SCHEMA_IN_APP_EVENTS_REPORT,
    },
    {
        'report_name': 'ad-revenue-raw-retarget', 'data_freshness': 'daily', 'type': 'raw',
        'partitioning_field': 'Event_Time', 'schema': table_schemas.SCHEMA_IN_APP_EVENTS_REPORT,
    },
    # {
    #     'report_name': 'blocked_installs_report', 'data_freshness': 'real_time', 'type': 'raw',
    #     'partitioning_field': 'Event_Time', 
    # },
    # # Schema might be wrong
    # {
    #     'report_name': 'detection', 'data_freshness': 'real_time', 'type': 'raw', 
    #     'partitioning_field': 'Event_Time'
    # },
    # # Schema might be wrong
    # {
    #     'report_name': 'blocked_in_app_events_report', 'data_freshness': 'daily', 'type': 'raw', 
    #     'partitioning_field': 'Date'
    # },
    # {
    # # Schema might be wrong
    #     'report_name': 'fraud-post-inapps', 'data_freshness': 'daily', 'type': 'raw', 
    #     'partitioning_field': 'Date'
    # },
    # # Schema might be wrong
    # {
    #     'report_name': 'blocked_clicks_report', 'data_freshness': 'daily', 'type': 'raw', 
    #     'partitioning_field': 'Date'
    # },
    {
        'report_name': 'blocked_install_postbacks', 'data_freshness': 'real_time', 'type': 'raw',
        'partitioning_field': 'Event_Time', 'schema': table_schemas.SCHEMA_IN_APP_EVENTS_POSTBACKS,
    },
    {
        'report_name': 'postbacks', 'data_freshness': 'daily', 'type': 'raw',
        'partitioning_field': 'Event_Time', 'schema': table_schemas.SCHEMA_IN_APP_EVENTS_POSTBACKS,
    },
    {
        'report_name': 'in-app-events-postbacks', 'data_freshness': 'daily', 'type': 'raw',
        'partitioning_field': 'Event_Time', 'schema': table_schemas.SCHEMA_IN_APP_EVENTS_POSTBACKS,
    },
    {
        'report_name': 'retarget_install_postbacks', 'data_freshness': 'real_time', 'type': 'raw',
        'partitioning_field': 'Event_Time', 'schema': table_schemas.SCHEMA_IN_APP_EVENTS_POSTBACKS,
    },
    {
        'report_name': 'retarget_in_app_events_postbacks', 'data_freshness': 'real_time', 'type': 'raw',
        'partitioning_field': 'Event_Time', 'schema': table_schemas.SCHEMA_IN_APP_EVENTS_POSTBACKS,
    },
    {
        'report_name': 'daily_report', 'data_freshness': 'daily', 'type': 'aggregate',
        'partitioning_field': 'Date', 'schema': table_schemas.SCHEMA_DAILY_REPORT,
    },
    # # # Schema might be wrong
    # # {
    # #     'report_name': 'partners_report', 'data_freshness': 'daily', 'type': 'aggregate', 'partitioning_field': 'Date'
    # # }, 
    # # {
    # #     'report_name': 'partners_by_date_report', 'data_freshness': 'daily', 'type': 'aggregate',
    # #     'partitioning_field': 'Date', 
    # #     # 'schema': table_schemas.SCHEMA,
    # # },
    # # # Schema might be wrong
    # # {
    # #     'report_name': 'geo_report', 'data_freshness': 'daily', 'type': 'aggregate', 
    # #     'partitioning_field': 'Date', 
    # #     # 'schema': table_schemas.SCHEMA,
    # # }, 
    # # {
    # #     'report_name': 'geo_by_date_report', 'data_freshness': 'daily', 'type': 'aggregate',
    # #     'partitioning_field': 'Date', 
    # #     # 'schema': table_schemas.SCHEMA,
    # # },
    {
        'report_name': 'conversion-studio-schema', 'data_freshness': 'daily', 'type': 'skan',
        'partitioning_field': 'last_config_change', 'schema': table_schemas.SCHEMA_SKAN_CONVERSION_STUDIO,
    },
    # {
    #     'report_name': 'cohort_user_acquisition', 
    #     'data_freshness': 'daily', 
    #     'type': 'cohort',
    #     'partitioning_field': 'Date', 
    #     # 'schema': table_schemas.SCHEMA_DAILY_REPORT,
    #     'cohort_attributes': {
    #         'kpis': ['users'],
    #         'aggregation_type': 'cumulative',
    #         'groupings': ["date"],
    #     }
    # },
    # {
    #     'report_name': 'cohort_retargeting', 
    #     'data_freshness': 'daily', 
    #     'type': 'cohort',
    #     'partitioning_field': 'Date', 
    #     # 'schema': table_schemas.SCHEMA_DAILY_REPORT,
    #     'cohort_attributes': {
    #         'kpis': ['users'],
    #         'aggregation_type': 'cumulative',
    #         'groupings': ["date"],
    #     }
    # },
    # {
    #     'report_name': 'cohort_unified', 
    #     'data_freshness': 'daily', 
    #     'type': 'cohort',
    #     'partitioning_field': 'Date', 
    #     # 'schema': table_schemas.SCHEMA_DAILY_REPORT,
    #     'cohort_attributes': {
    #         'kpis': ['users'],
    #         'aggregation_type': 'cumulative',
    #         'groupings': ["date"],
    #     }
    # },
]


def access_secret(secret_id, project_id):
    """
    Accesses a secret from Google Cloud Secret Manager.
    """
    client = secretmanager_v1.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode('UTF-8')

def remove_items_by_key_value(dict_list, key, value):
    """
    Filters a list of dicts, removing all dicts where the key contains a specific value
    """
    return [item for item in dict_list if item.get(key) == value]

def write_local_csv(csv_data, platform, report_name):
    """
    Write the CSVs in a local environment for debugging
    """
    import os
    
    try:
        # Create temp folder to hold the CSV files locally
        if not os.path.exists('csv_reports_appsflyer'):
            os.makedirs('csv_reports_appsflyer')

        with open(f'csv_reports_appsflyer/{platform}_{report_name}.csv', 'w', newline='', encoding='utf-8-sig') as file:
            writer = csv.writer(file)
            csv_reader = csv.reader(csv_data, delimiter=',')
            for row in csv_reader:
                writer.writerow(row)
        logger.info(f'CSV file successfully generated for {platform} - {report_name}.')
    except Exception as e:
        message = f'Error generating CSV file for {platform} - {report_name}: {e}'
        send_slack_notification(message)
        logger.error(message)

def get_table_columns_and_types(bq_client, table_id):
    dataset_id = table_id.split('.')[1]
    table_name = table_id.split('.')[-1]
    query = f"""
    SELECT column_name, data_type
    FROM `{bq_client.project}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = '{table_name}'
    """
    query_job = bq_client.query(query)
    results = query_job.result()
    if not results:
        message = f"Table {dataset_id}.{table_name} does not exist"
        send_slack_notification(message)
        raise Exception(message)
    columns_and_types = {row['column_name']: row['data_type'] for row in results}
    return columns_and_types

def create_target_table_with_schema(bq_client, staging_table_id, schema, report):
    logger.info("------ Creating Target Table ----------")
    table = bigquery.Table(staging_table_id, schema=schema)
    partition_type = report["data_freshness"] # data_freshness is essentially real_time, continuous, daily etc..
    partition_time = partitioning_type[partition_type]
    table.time_partitioning = bigquery.TimePartitioning(
    type_=bigquery.TimePartitioningType.HOUR if partition_time == 'HOUR' else bigquery.TimePartitioningType.DAY,
    field=report["partitioning_field"],  # name of column to use for partitioning
    )
    bq_client.create_table(table, exists_ok=True)
    logger.info(f"Created target table for {staging_table_id} with {partition_time} partitioning on {report['partitioning_field']}")


def create_staging_table_with_schema(bq_client, staging_table_id, schema):
    table = bigquery.Table(staging_table_id, schema=schema)
    bq_client.create_table(table, exists_ok=True)

def load_to_staging_table(bq_client, csv_data, staging_table_id, schema):
    create_staging_table_with_schema(bq_client, staging_table_id, schema)
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=schema
    )
    
    csv_data.seek(0)
    
    with io.BytesIO(csv_data.read().encode('utf-8')) as csv_binary:
        job = bq_client.load_table_from_file(csv_binary, staging_table_id, job_config=job_config)
        job.result()

def merge_staging_to_target(bq_client, staging_table_id, target_table_id, partitioning_field, schema, report):
    columns_and_types = get_table_columns_and_types(bq_client, target_table_id)
    all_columns = list(columns_and_types.keys())
    if len(all_columns) == 0:
        create_target_table_with_schema(bq_client, target_table_id, schema, report)
        columns_and_types = get_table_columns_and_types(bq_client, target_table_id)
        all_columns = list(columns_and_types.keys())
    print("------- COLUMNS ---------")
    print(all_columns)
    
    update_set_clause = ', '.join([
        f"T.{col} = S.{col}" for col in all_columns
    ])
    insert_columns = ', '.join(all_columns)
    insert_values = ', '.join([
        f"S.{col}" for col in all_columns
    ])
    
    on_clause = ' AND '.join([
        f"T.{col} = S.{col}" for col in all_columns
    ])

    query = f"""
    INSERT INTO `{target_table_id}`
    SELECT *
    FROM `{staging_table_id}`
    EXCEPT DISTINCT
    SELECT *
    FROM `{target_table_id}`;
    """

    # query = f"""
    # MERGE `{target_table_id}` T
    # USING `{staging_table_id}` S
    # ON {on_clause}
    # WHEN MATCHED THEN
    #   UPDATE SET {update_set_clause}
    # WHEN NOT MATCHED THEN
    #   INSERT ({insert_columns})
    #   VALUES ({insert_values});
    # """

    try:
        query_job = bq_client.query(query)
        query_job.result()
        logger.info(f"Success: The merge_staging_to_target Query succeded.")
    except Exception as e:
        message = f"Failure: The merge_staging_to_target Query failed: {e}"
        send_slack_notification(message)
        logger.error(message)
        return


def write_to_bq(
    bq_client,
    csv_data,
    staging_table_id,
    table_id,
    partitioning_field,
    schema,
    report
):
    try:
        load_to_staging_table(bq_client, csv_data, staging_table_id, schema)
        logger.info(f'Successfully loaded data to staging table {staging_table_id}')
    except Exception as e:
        message = f'Error loading to staging table {staging_table_id}: {e}'
        send_slack_notification(message)
        logger.error(message)
    try:
        merge_staging_to_target(bq_client, staging_table_id, table_id, partitioning_field, schema, report)
        logger.info(f'Successfully merged data from staging table {staging_table_id} to target table {table_id}')
    except Exception as e:
        message = f'Error merging data from staging table {staging_table_id} to target table {table_id}: {e}'
        send_slack_notification(message)
        logger.error(message)

def parse_date(date_str):
    try:
        return parse(date_str)
    except ValueError:
        message = f'Invalid date format: {date_str}'
        send_slack_notification(message)
        raise Exception('Invalid date format')

def generate_reports(
    product_name,
    data_freshness,
    app_ids,
    dataset,
    reports,
    api_token,
    start_date,
    end_date,
):
    bq_client = bigquery.Client(project=project_id)
    num_request = 0

    def process_data(csv_data):
        if DEBUG:
            write_local_csv(csv_data, platform, report_name)
        else:
            staging_table_id = f'{project_name}.{staging_dataset}.{product_name}_{platform}_{report_name}'
            table_id = f'{project_name}.{dataset}.{platform}_{report_name}'
            if dataset in table_id:
                print(f" - TARGET TABLE NAME: {table_id}")
                print(f" - STAGING TABLE NAME: {staging_table_id}")
            write_to_bq(
                bq_client,
                csv_data,
                staging_table_id,
                table_id,
                report['partitioning_field'],
                report['schema'],
                report
            )

    for app in app_ids:
        params = {}
        platform = app['platform']
        app_id = app['app_id']
        logger.info(f'Starting platform {platform} for app_id {app_id}')

        for report in reports:
            if num_request > LIMIT_NUM_REQUEST:
                message = 'Hit the limit of number of api requests'
                send_slack_notification(message)
                logging.warning(message)
                # Further processing if we hit the limit
            report_name = report['report_name']
            logger.info(f'Generating report: {report_name}')

            report_type = None
            if report['type'] == 'raw':
                report_type = 'raw-data'
            elif report['type'] == 'aggregate':
                report_type = 'agg-data'
            elif report['type'] == 'cohort':
                report_type = 'cohort'
            elif report['type'] == 'skan':
                report_type = 'skan'
            else:
                message = 'Invalid report type (raw/aggregate/cohort/skan)'
                send_slack_notification(message)
                raise Exception(message)

            api_endpoint = ''
            if report_type == 'cohort':
                api_endpoint = f'https://hq1.appsflyer.com/api/cohorts/v1/data/app/{app_id}'
            elif report_type == 'skan':
                api_endpoint = f'https://hq1.appsflyer.com/api/skan-pull-cs-api/v1/conversion-studio-schema/app/{app_id}'
            else:
                api_endpoint = f'https://hq1.appsflyer.com/api/{report_type}/export/app/{app_id}/{report_name}/v5'
            
            now = datetime.now()
            start_of_previous_hour = (now - timedelta(hours=2)).replace(minute=0, second=0, microsecond=0)
            previous_day = (now - timedelta(days=1))

            if start_date and end_date:
                start_date_dt = parse_date(start_date)
                end_date_dt = parse_date(end_date)
                
                if (
                    "T" in start_date or " " in start_date or ":" in start_date or
                    "T" in end_date or " " in end_date or ":" in end_date
                ) and data_freshness != 'daily':
                    params = {
                        "from": start_date_dt.strftime("%Y-%m-%d %H:%M:%S"),
                        "to": end_date_dt.strftime("%Y-%m-%d %H:%M:%S")
                    }
                else:
                    params = {
                        "from": start_date_dt.strftime("%Y-%m-%d"),
                        "to": end_date_dt.strftime("%Y-%m-%d")
                    }
            else:
                if data_freshness == 'daily':
                    params = {
                        "from": previous_day.strftime("%Y-%m-%d"),
                        "to": now.strftime("%Y-%m-%d")
                    }
                else:
                    params = {
                        "from": start_of_previous_hour.strftime("%Y-%m-%d %H:%M:%S"),
                        "to": now.strftime("%Y-%m-%d %H:%M:%S")
                    }

            headers = {
                'Authorization': f'Bearer {api_token}'
            }

            logger.info(f'Request params: {params}')
            logger.info(f'API endpoint: {api_endpoint}')
            if report_type == 'cohort':
                headers["accept"] = "application/json"
                headers["content-type"] = "application/json"
                params["aggregation_type"] = report["cohort_attributes"]["aggregation_type"]
                params["groupings"] = report["cohort_attributes"]["groupings"]
                params["kpis"] = report["cohort_attributes"]["kpis"]
                if "user_acquisition" in report_name:
                    params["cohort_type"] = "user_acquisition"
                elif "retargeting" in report_name:
                    params["cohort_type"] = "retargeting"
                elif "unified" in report_name:
                    params["cohort_type"] = "unified"
                logging.info(f'Request params: {params}')
                response = requests.post(api_endpoint, json=params, headers=headers)
            elif report_type == 'skan':
                headers["accept"] = "text/csv"
                logging.info(f'Request params: {params}')
                response = requests.get(api_endpoint, params=params, headers=headers, stream=True)
            else:
                logging.info(f'Request params: {params}')
                response = requests.get(api_endpoint, params=params, headers=headers, stream=True)

            message = 'Failed {} {} {} with status code {}.\nReason: {}.\nHeaders: {}.\nContent:{}'.format(
                product_name,
                app['platform'],
                report_name,
                response.status_code,
                response.reason,
                response.headers,
                response.content,
            )

            num_request += 1

            num_batch = 1
            # TODO: send warning if request reaches 200k lines, as this means some rows were cut off
            if response.status_code == 200:
                csv_batch_data = io.StringIO()
                line_iterator = response.iter_lines()
                num_rows = 0
                first_line = None
                while True:
                    try:
                        line = next(line_iterator)
                        line = line.decode('utf-8')
                        if (num_rows == 0) and (first_line is None):
                            first_line = line
                            if first_line.startswith('\ufeff'):
                                first_line = first_line[1:]
                                csv_batch_data.write(first_line + '\n')
                        else:
                            csv_batch_data.write(line + '\n')
                        num_rows += 1

                        if num_rows == BATCH_SIZE:
                            data = csv_batch_data.getvalue().split('\n') 
                            logging.info(f"Start batch {num_batch}")
                            logging.info(f"Preview data header {data[0]}")
                            logging.info(f"Preview data first column {data[1]}")
                            process_data(csv_batch_data)
                            num_batch += 1
                            num_rows = 0
                            csv_batch_data = io.StringIO()
                            csv_batch_data.write(first_line + '\n')
                    except StopIteration:
                        if num_rows > 0:
                            data = csv_batch_data.getvalue().split('\n')
                            logging.info(f"Start batch {num_batch}")
                            logging.info(f"Preview data header {data[0]}")
                            logging.info(f"Preview data first column {data[1]}")
                            logging.info(f"This is last batch with size {len(data)}")
                            process_data(csv_batch_data)

                        logging.info("Done all batches")
                        break
                    except Exception as e:
                        message = f"Error while processing batch {num_batch} of report {report_name}: {str(e)}"
                        send_slack_notification(message)
                        logging.error(message)
                        break
            else:
                message = 'Failed {} {} {} with status code {}.\nReason: {}.\nHeaders: {}.\nContent:{}'.format(
                    product_name,
                    app['platform'],
                    report_name,
                    response.status_code,
                    response.reason,
                    response.headers,
                    response.content,
                )
                send_slack_notification(message)
                logger.error(message)

@functions_framework.cloud_event
def entry_pubsub(cloud_event):
    data = base64.b64decode(cloud_event.data['message']['data']).decode('utf-8')
    message = json.loads(data)

    data_freshness_to_run = []
    if "data_freshness" in message:
        data_freshness = message["data_freshness"]
        if data_freshness in ['real_time', 'continuous', 'daily']:
            data_freshness_to_run.append(data_freshness)
        elif data_freshness == 'all':
            data_freshness_to_run = ['real_time', 'continuous', 'daily']
        else:
            message = 'Invalid data_freshness'
            send_slack_notification(message)
            raise Exception(message)

    start_date = None
    end_date = None
    if "start_date" in message and "end_date" in message:
        try:
            start_date = parse(message["start_date"]).isoformat()
            end_date = parse(message["end_date"]).isoformat()
        except ValueError as e:
            message = 'Invalid date format'
            send_slack_notification(message)
            raise Exception('Invalid date format') from e

    for data_freshness in data_freshness_to_run:
        logger.info(f'Running for {data_freshness} data freshness')
        filtered_reports = remove_items_by_key_value(reports, 'data_freshness', data_freshness)

        for product_name in products:
            generate_reports(
                product_name,
                data_freshness,
                products[product_name]['app_ids'],
                products[product_name]['dataset'],
                filtered_reports,
                access_secret(
                    products[product_name]['secret_manager_api_key_id'],
                    project_id,
                ),
                start_date,
                end_date,
            )

    logger.info('Execution successfully finished')

generate_reports(
                product_name='cats_and_soup',
                data_freshness='daily',
                app_ids=products['cats_and_soup']['app_ids'],
                dataset=products['cats_and_soup']['dataset'],
                reports=reports,
                api_token=access_secret(
                    products['cats_and_soup']['secret_manager_api_key_id'],
                    project_id,
                ),
                start_date='2024-01-10',
                end_date='2024-01-14',
            )

# generate_reports(
#                 product_name='tales_of_crown',
#                 data_freshness='daily',
#                 app_ids=products['tales_of_crown']['app_ids'],
#                 dataset=products['tales_of_crown']['dataset'],
#                 reports=reports,
#                 api_token=access_secret(
#                     products['tales_of_crown']['secret_manager_api_key_id'],
#                     project_id,
#                 ),
#                 start_date='2024-06-27',
#                 end_date='2024-07-03',
#             )