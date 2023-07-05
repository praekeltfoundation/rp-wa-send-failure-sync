import os
from datetime import datetime, timedelta
from urllib.parse import urljoin

import psycopg2
import requests

CHANNEL_ID = os.environ.get("CHANNEL_ID", "11")
HUB_URL = os.environ.get("HUB_URL")
HUB_TOKEN = os.environ.get("HUB_TOKEN")

DB_NAME = os.environ["DATABASE_NAME"]
DB_USER = os.environ["LOGIN"]
DB_PASSWORD = os.environ.get("PASSWORD")
DB_HOST = os.environ["HOST"]
DB_PORT = int(os.environ["DB_PORT"])


def get_send_errors(error_date):
    cursor.execute(
        """
        SELECT contacts_contacturn.path, MAX(channels_channellog.created_on)
        FROM channels_channellog
            INNER JOIN msgs_msg
                ON channels_channellog.msg_id = msgs_msg.id
            INNER JOIN contacts_contacturn
                ON msgs_msg.contact_urn_id = contacts_contacturn.id
        WHERE channels_channellog.created_on::date = %s
        AND channels_channellog.channel_id = %s
        AND channels_channellog.is_error = TRUE
        AND channels_channellog.response LIKE '%%"code":1013%%'
        GROUP BY contacts_contacturn.path
        """,
        (error_date, CHANNEL_ID),
    )
    return [(urn, error_timestamp) for urn, error_timestamp in cursor]


def send_error_to_hub(contact_id, timestamp):
    headers = {
        "Authorization": "Token {}".format(HUB_TOKEN),
        "Content-Type": "application/json",
    }
    response = requests.post(
        urljoin(HUB_URL, f"/api/v2/deliveryfailure/{contact_id}/"),
        headers=headers,
        json={
            "contact_id": contact_id,
            "timestamp": timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        },
    )
    response.raise_for_status()
    return response.status_code


if __name__ == "__main__":
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )
    cursor = conn.cursor()

    yesterday = datetime.now() - timedelta(1)
    send_errors = get_send_errors(datetime.strftime(yesterday, "%Y-%m-%d"))

    for contact_id, error_timestamp in send_errors:
        print(f"Sending *{contact_id[-4:]} - {error_timestamp}", end=" ")
        status_code = send_error_to_hub(contact_id, error_timestamp)
        print(f"Result: {status_code}")

    print(f"Completed: {len(send_errors)}")
