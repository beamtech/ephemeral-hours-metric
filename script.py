from kubernetes import client, config
from datadog import initialize, api
import datetime
import os
import time
import calendar

options = {
    "api_key": "DD-API-KEY",
    "app_key": "DD-APP-KEY",
}
initialize(**options)
cluster = "ephemeral21a"


def get_ephemerals():
    """Returns all ephemerals from the cluster."""
    config.load_kube_config()
    eph = client.CustomObjectsApi()
    return eph.list_cluster_custom_object(
        group="k8s.beam.dental", version="v1", plural="ephemerals"
    )


def send_hours_metric_to_datadog(curr_time: str, eph_lifespan: str, host: str):
    """
    Sends a custom metric to datadog.
    :param curr_time: Current time.
    :param eph_lifespan: Total amount of hours all ephemerals have been running for this month.
    :param host: Cluster name.
    """

    api.Metric.send(
        [
            {
                "metric": "eph.hours",
                "type": "count",
                "points": (curr_time, eph_lifespan),
                "host": host,
                "tags": [],
            }
        ]
    )


def send_eph_count_metric_to_datadog(curr_time: str, num_of_ephemerals: str, host: str):
    """
    Sends a custom metric to datadog.
    :param curr_time: Current time.
    :param num_of_ephemerals: Total amount of ephemerals that are currently running.
    :param host: Cluster name.
    """

    api.Metric.send(
        [
            {
                "metric": "eph.eph_count",
                "type": "gauge",
                "points": (curr_time, num_of_ephemerals),
                "host": host,
                "tags": [],
            }
        ]
    )


def ephemeral_hours():
    ret = get_ephemerals()
    now = time.mktime(datetime.datetime.now().timetuple())
    # This is to determine the first date of the current month and coverts it to seconds
    todayDate = datetime.date.today()
    firstOfTheMonth = int(todayDate.replace(day=1).strftime("%s"))
    # iterating through the ephemerals
    total_time = 0
    for i in ret["items"]:
        creationTimestamp = i["metadata"]["creationTimestamp"]
        ttl = i["status"]["ttl"]
        namespace = i["metadata"]["name"]
        # Converting creationTimestamp of the ephemeral to seconds
        timestamp = time.strptime(creationTimestamp, "%Y-%m-%dT%H:%M:%SZ")
        unix_time_utc = calendar.timegm(timestamp)
        # If the creationTimestamp is before the first of the current month,
        # then the creationTimestamp becomes the first of the current month
        if unix_time_utc < firstOfTheMonth:
            unix_time_utc = firstOfTheMonth
        # Subtract the creation time from now
        lifespan = now - unix_time_utc
        # Sum total_time, which will get sent to datadog
        total_time = total_time + lifespan
    total_time_seconds = total_time
    total_time_minutes = total_time_seconds / 60
    total_time_hours = total_time_minutes / 24
    print(
        f"total_eph_uptime for month: {int(total_time_seconds)} seconds or {int(total_time_minutes)} minutes or {int(total_time_hours)} hours."
    )
    send_hours_metric_to_datadog(
        curr_time=time.time(), eph_lifespan=total_time_hours, host=cluster
    )


def ephemeral_count():
    ret = get_ephemerals()
    list = []

    for i in ret["items"]:
        namespace = i["metadata"]["name"]

        list.append(namespace)

    num_of_ephemerals = str(len(list))
    send_eph_count_metric_to_datadog(
        curr_time=time.time(), num_of_ephemerals=num_of_ephemerals, host=cluster
    )


if __name__ == "__main__":
    ephemeral_hours()
    ephemeral_count()
