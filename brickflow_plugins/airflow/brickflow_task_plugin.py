from typing import Optional
import datetime
import pendulum
import re

try:
    from airflow import macros
    from airflow.models import BaseOperator
    from airflow.utils.context import Context
except ImportError:
    raise ImportError(
        "You must install airflow to use airflow plugins, "
        "please try pip install brickflow[apache-airflow]"
    )

from jinja2 import Environment
from brickflow.context import ctx
from brickflow.engine.hooks import BrickflowTaskPluginSpec
from brickflow.engine.task import brickflow_task_plugin_impl, Task, TaskResponse
from brickflow.engine.workflow import Workflow

from brickflow_plugins import log
from brickflow_plugins.airflow.context import get_task_context
from brickflow_plugins.airflow.operators import get_modifier_chain
from brickflow_plugins.secrets import BrickflowSecretsBackend


def epoch_to_pendulum_datetime(time: Optional[str]) -> Optional[pendulum.DateTime]:
    if time is None:
        log.info("Input time is None")
        return None
    log.info("Input time: %s", time)
    if isinstance(time, str):
        if re.match(r"^-?\d+$", time):  # Check if the string is a valid integer (epoch)
            epoch_time = int(time)
            return pendulum.from_timestamp(epoch_time / 1000)
        try:
            # Attempt to parse the string as a timestamp
            return pendulum.parse(time)
        except ValueError as e:
            log.error("Error parsing time string: %s", e)
            return None
    try:
        # If time is not a string, attempt to convert assuming it's an epoch integer
        return pendulum.from_timestamp(int(time) / 1000)
    except (ValueError, TypeError) as e:
        log.error("Error converting non-string time: %s", e)
        return None


class AirflowOperatorBrickflowTaskPluginImpl(BrickflowTaskPluginSpec):
    @staticmethod
    @brickflow_task_plugin_impl(tryfirst=True)
    def handle_results(
        resp: "TaskResponse", task: "Task", workflow: "Workflow"
    ) -> "TaskResponse":
        log.info(
            "using AirflowOperatorBrickflowTaskPlugin for handling results for task: %s",
            task.task_id,
        )

        BrickflowTaskPluginSpec.handle_user_result_errors(resp)

        _operator = resp.response

        if not isinstance(_operator, BaseOperator):
            return resp

        operator_modifier_chain = get_modifier_chain()
        # modify any functionality of operators and then
        _operator = operator_modifier_chain.modify(_operator, task, workflow)

        if hasattr(_operator, "log"):
            # overwrite the operator logger if it has one to the brickflow logger
            setattr(_operator, "_log", ctx.log)
        log.info(
            "Handling both epoch and timestamp string: %s",
            epoch_to_pendulum_datetime(ctx.brickflow_start_time(debug=None)),
        )

        context: Context = get_task_context(
            task.task_id,
            _operator,
            workflow.schedule_quartz_expression,
            epoch_to_pendulum_datetime(ctx.brickflow_start_time(debug=None)),
            tz=workflow.timezone,
        )

        env: Optional[Environment] = Environment()
        env.globals.update({"macros": macros, "ti": context})
        with BrickflowSecretsBackend():
            _operator.render_template_fields(context, jinja_env=env)
            op_resp = _operator.execute(context)
            return TaskResponse(
                response=op_resp,
                push_return_value=_operator.do_xcom_push,
            )
