import abc
import json
import logging
from http import HTTPStatus
from typing import Callable, Union
from pydantic import SecretStr
from brickflow.context import ctx

import requests
from airflow.models import Connection
from airflow.sensors.base import BaseSensorOperator
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from requests import HTTPError

from datetime import datetime, timedelta
from dateutil.parser import parse  # type: ignore[import-untyped]
import time
import pytz
from brickflow_plugins import log
from databricks.sdk import WorkspaceClient
from datetime import datetime, timedelta,timezone


class DagSchedule:
    def get_schedule(self, wf_id: str, **args):
        """
        Function that the sensors defined while deriving this class should
        override.
        """
        raise Exception("Override me.")

    def get_task_run_status(self, wf_id: str, task_id: str, run_date=None, **args):
        """
        Function that the sensors defined while deriving this class should
        override.
        """
        raise Exception("Override me.")


# TODO: implement Delta Json


class AirflowClusterAuth(abc.ABC):
    @abc.abstractmethod
    def get_access_token(self) -> str:
        pass

    @abc.abstractmethod
    def get_airflow_api_url(self) -> str:
        pass

    @abc.abstractmethod
    def get_version(self) -> str:
        pass


class AirflowProxyOktaClusterAuth(AirflowClusterAuth):
    def __init__(
            self,
            oauth2_conn_id: str,
            airflow_cluster_url: str,
            airflow_version: str = None,
            get_airflow_version_callback: Callable[[str, str], str] = None,
    ):
        self._airflow_version = airflow_version
        self._get_airflow_version_callback = get_airflow_version_callback
        self._oauth2_conn_id = oauth2_conn_id
        self._airflow_url = airflow_cluster_url.rstrip("/")
        if airflow_version is None and get_airflow_version_callback is None:
            raise Exception(
                "Either airflow_version or get_airflow_version_callback must be provided"
            )

    def get_okta_conn(self):
        return Connection.get_connection_from_secrets(self._oauth2_conn_id)

    def get_okta_url(self) -> str:
        conn_type = self.get_okta_conn().conn_type
        host = self.get_okta_conn().host
        schema = self.get_okta_conn().schema
        return f"{conn_type}://{host}/{schema}"

    def get_okta_client_id(self) -> str:
        return self.get_okta_conn().login

    def get_okta_client_secret(self) -> str:
        return self.get_okta_conn().get_password()

    def get_access_token(self) -> str:
        okta_url = self.get_okta_url()
        client_id = self.get_okta_client_id()
        client_secret = self.get_okta_client_secret()

        payload = (
                "client_id="
                + client_id
                + "&client_secret="
                + client_secret
                + "&grant_type=client_credentials"
        )
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "cache-control": "no-cache",
        }
        response = requests.post(okta_url, data=payload, headers=headers, timeout=600)
        if (
                response.status_code < HTTPStatus.OK
                or response.status_code > HTTPStatus.PARTIAL_CONTENT
        ):
            log.error(
                "Failed request to Okta for JWT status_code={} response={} client_id={}".format(
                    response.status_code, response.text, client_id
                )
            )
        token_data = response.json()["access_token"]
        return token_data

    def get_airflow_api_url(self) -> str:
        # TODO: templatize this to a env variable
        return self._airflow_url

    def get_version(self) -> str:
        if self._airflow_version is not None:
            return self._airflow_version
        else:
            return self._get_airflow_version_callback(
                self._airflow_url, self.get_access_token()
            )


class AirflowScheduleHelper(DagSchedule):
    def __init__(self, airflow_auth: AirflowClusterAuth):
        self._airflow_auth = airflow_auth

    def get_task_run_status(
            self, wf_id: str, task_id: str, latest=False, run_date=None, **kwargs
    ):
        token_data = self._airflow_auth.get_access_token()
        api_url = self._airflow_auth.get_airflow_api_url()
        version_nr = self._airflow_auth.get_version()
        dag_id = wf_id
        headers = {
            "Content-Type": "application/json",
            "cache-control": "no-cache",
            "Authorization": "Bearer " + token_data,
        }
        o_task_status = "UKN"
        session = requests.Session()
        retries = Retry(
            total=5, backoff_factor=1, status_forcelist=[502, 503, 504, 500]
        )
        session.mount("https://", HTTPAdapter(max_retries=retries))
        if version_nr.startswith("1."):
            log.info("this is 1.x cluster")
            url = (
                    api_url
                    + "/api/experimental"
                    + "/dags/"
                    + dag_id
                    + "/dag_runs/"
                    + run_date
                    + "/tasks/"
                    + task_id
            )
        else:
            url = (
                    api_url
                    + "/api/v1/dags/"
                    + dag_id
                    + "/dagRuns/scheduled__"
                    + run_date
                    + "/taskInstances/"
                    + task_id
            )

        log.info(f"url= {url.replace(' ', '')}")
        response = session.get(url.replace(" ", ""), headers=headers)

        log.info(
            f"response.status_code= {response.status_code} response.text= {response.text}"
        )
        if response.status_code == 200:
            log.info(f"response= {response.text}")
            json_obj = json.loads(response.text)
            if type(json_obj) == dict:
                o_task_status = json_obj["state"]

            return o_task_status

        return o_task_status


class TaskDependencySensor(BaseSensorOperator):
    def __init__(
            self,
            external_dag_id,
            external_task_id,
            databricks_host: str,
            databricks_token: Union[str, SecretStr],
            airflow_auth: AirflowClusterAuth,
            allowed_states=None,
            execution_delta=None,
            execution_delta_json=None,
            latest=False,
            poke_interval=60,
            *args,
            **kwargs,
    ):
        super(TaskDependencySensor, self).__init__(*args, **kwargs)
        self._airflow_auth = airflow_auth
        self.allowed_states = allowed_states or ["success"]
        if execution_delta_json and execution_delta:
            raise Exception(
                "Only one of `execution_date` or `execution_delta_json` maybe provided to Sensor; not more than one."
            )
        self.databricks_host = databricks_host
        self.databricks_token = (
            databricks_token
            if isinstance(databricks_token, SecretStr)
            else SecretStr(databricks_token)
        )
        self.external_dag_id = external_dag_id
        self.external_task_id = external_task_id
        self.allowed_states = allowed_states
        self.execution_delta = execution_delta
        self.execution_delta_json = execution_delta_json
        self.latest = latest
        self.poke_interval = poke_interval
        self._poke_count = 0
        self._workspace_obj = WorkspaceClient(
            host=self.databricks_host, token=self.databricks_token.get_secret_value()
        )

    def get_execution_start_time_unix_milliseconds(self) -> int:

        run_id = ctx.dbutils_widget_get_or_else("brickflow_parent_run_id", None)
        if run_id is None:
            raise TaskDependencySensor(
                "run_id is empty, brickflow_parent_run_id parameter is not found "
                "or no value present"
            )

        run = self._workspace_obj.jobs.get_run(run_id=run_id)

        # Convert Unix timestamp in milliseconds to datetime object to easily incorporate the delta
        start_time = datetime.fromtimestamp(run.start_time / 1000)
        execution_start_time = start_time.replace(second=0, microsecond=0)
        # Convert datetime object back to Unix timestamp in miliseconds
        execution_start_time_unix_miliseconds = int(
            execution_start_time.timestamp() * 1000
        )

        execution_start_time_datetime = datetime.fromtimestamp(execution_start_time_unix_miliseconds / 1000).replace(tzinfo=timezone.utc)

        execution_start_time_datetime = execution_start_time_datetime.strftime('%Y-%m-%dT%H:%M:%SZ')

        self.log.info(f"This workflow started at {execution_start_time}")
        self.log.info(
            f"{execution_start_time} in UNIX miliseconds is {execution_start_time_datetime}"
        )
        return execution_start_time_datetime

    def get_execution_stats(self,execution_window_tz):
        """Function to get the execution stats for task_id within a execution delta window

        Returns:
            string: state of the desired task id and dag_run_id (success/failure/running)
        """
        latest = self.latest
        okta_token = self._airflow_auth.get_access_token()
        api_url = self._airflow_auth.get_airflow_api_url()
        af_version = self._airflow_auth.get_version()
        external_dag_id = self.external_dag_id
        external_task_id = self.external_task_id
        execution_delta = self.execution_delta
        execution_window_tz = execution_window_tz
        # log.info(f"brickflow start date {execution_window_tz}")
        # execution_start_time = datetime.fromisoformat(execution_window_tz)
        #
        # execution_start_time = execution_start_time.replace(second=0, microsecond=0)
        # execution_start_time_str = execution_start_time.strftime('%Y-%m-%dT%H:%M:%S%z')



        headers = {
            "Content-Type": "application/json",
            "cache-control": "no-cache",
            "Authorization": "Bearer " + okta_token,
        }
        if af_version.startswith("1."):
            log.info("this is 1.x cluster")
            url = (
                    api_url
                    + "/api/experimental"
                    + "/dags/"
                    + external_dag_id
                    + "/dag_runs/"
            )
        else:
            # Airflow API for 2.X version limits 100 records, so only picking runs within the execution window provided
            url = (
                    api_url
                    + "/api/v1/dags/"
                    + external_dag_id
                    + f"/dagRuns/scheduled_{execution_window_tz}"
            )

        # log.info(f"URL to poke for dag runs {url}")
        # response = requests.request("GET", url, headers=headers)
        # if response.status_code == 401:
        #     raise Exception(
        #         f"No Runs found for {external_dag_id} dag after {execution_window_tz}, Please check upstream dag"
        #     )
        # response.raise_for_status()
        # list_of_dictionaries = response.json()
        # list_of_dictionaries = response.json()["dag_runs"]
        # list_of_dictionaries = sorted(
        #     list_of_dictionaries, key=lambda k: k["execution_date"], reverse=True
        # )
        # if af_version.startswith("1."):
        #     # For airflow 1.X Execution date is needed to check the status of the task
        #     dag_run_id = list_of_dictionaries[0]["execution_date"]
        # else:
        #     # For airflow 2.X or higher dag_run_id is needed to check the status of the task
        #     dag_run_id = list_of_dictionaries[-1]["dag_run_id"]
        #     if latest:
        #         # Only picking the latest run id if latest flag is True
        #         dag_run_id = list_of_dictionaries[0]["dag_run_id"]
        # log.info(f"Latest run for the dag is with execution date of  {dag_run_id}")
        # log.info(
        #     f"Poking {external_dag_id} dag for {dag_run_id} run_id status as latest flag is set to {latest} "
        # )
        if af_version.startswith("1."):
            task_url = url + "/taskInstances/{external_task_id}"

        else:
            task_url = (
                    url[: url.rfind("/")]
                    + f"/scheduled__{execution_window_tz}/taskInstances/{external_task_id}"
            )
        log.info(f"Pinging airflow API {task_url} for task status ")
        task_response = requests.request("GET", task_url, headers=headers)
        task_response.raise_for_status()
        task_state = task_response.json()["state"]
        return task_state

    def poke(self, context,execution_window_tz):
        log.info(f"executing poke.. {self._poke_count}")
        self._poke_count = self._poke_count + 1
        logging.info("Poking.. {0} round".format(str(self._poke_count)))
        task_status = self.get_execution_stats(execution_window_tz)
        log.info(f"task_status= {task_status}")
        return task_status

    def execute(self, context):
        """Function inherited from the BaseSensor Operator to execute the Poke Function

        Args:
            context (dictionary): instance of the airflow task

        Raises:
            Exception: If Upstream Dag is Failed
        """
        execution_start_time = datetime.strptime(self.get_execution_start_time_unix_milliseconds(), "%Y-%m-%dT%H:%M:%SZ")
        self.okta_token = self._airflow_auth.get_access_token()
        allowed_states = self.allowed_states
        external_dag_id = self.external_dag_id
        external_task_id = self.external_task_id
        execution_delta = self.execution_delta
        execution_window_tz = (execution_start_time + execution_delta).strftime(
            "%Y-%m-%dT%H:%M:%S%z"
        ) + "+00:00"
        log.info(
            f"Executing TaskDependency Sensor Operator to check successful run for {external_dag_id} dag, task {external_task_id} after {execution_window_tz} "
        )
        status = ""
        while status not in allowed_states:
            status = self.poke(context,execution_window_tz)
            if status == "failed":
                log.error(
                    f"Upstream dag {external_dag_id} failed at {external_task_id} task "
                )
                raise Exception("Upstream Dag Failed")
            elif status != "success":
                time.sleep(self.poke_interval)
        log.info(f"Upstream Dag {external_dag_id} is successful")


class AutosysSensor(BaseSensorOperator):
    def __init__(
            self,
            url: str,
            job_name: str,
            poke_interval: int,
            time_delta: Union[timedelta, dict] = {"days": 0},
            *args,
            **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.url = url
        self.job_name = job_name
        self.poke_interval = poke_interval
        self.time_delta = time_delta
        self.url = self.url + self.job_name

    """
        Takes in url, job_name, poke_interval, execution delta and airflow_cluster_auth as parameters 
        and sends a http get() request, checks the API response and exits the process 
        if the specified conditions are met.
        If not, waits for the given poke interval, then pokes again and again until the conditions
        are met or times out.
    """

    def poke(self, context):
        logging.info("Poking: " + self.url)

        headers = {
            "Accept": "application/json",
            "cache-control": "no-cache",
        }

        response = requests.get(
            self.url,
            headers=headers,
            verify=False,  # nosec
            timeout=10,
        )

        if response.status_code != 200:
            raise HTTPError(
                f"Request failed with '{response.status_code}' code. \n{response.text}"
            )
        else:
            status = response.json()["status"][:2].upper()

            last_end_timestamp = None
            if last_end_utc := response.json().get("lastEndUTC"):
                last_end_timestamp = parse(last_end_utc).replace(tzinfo=pytz.UTC)

            time_delta = (
                self.time_delta
                if isinstance(self.time_delta, timedelta)
                else timedelta(**self.time_delta)
            )

            execution_timestamp = parse(context["execution_date"])
            run_timestamp = execution_timestamp - time_delta

            if (
                    "SU" in status
                    and last_end_timestamp
                    and last_end_timestamp >= run_timestamp
            ):
                logging.info(
                    f"Last End: {last_end_timestamp}, Run Timestamp: {run_timestamp}"
                )
                logging.info("Success criteria met. Exiting")
                return True
            else:
                logging.info(
                    f"Last End: {last_end_timestamp}, Run Timestamp: {run_timestamp}"
                )
                time.sleep(self.poke_interval)
                logging.info("Poking again")
                AutosysSensor.poke(self, context)
