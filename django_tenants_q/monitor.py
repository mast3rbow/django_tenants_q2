from multiprocessing.process import current_process
from multiprocessing.queues import Queue

from django import core, db
from django.utils.translation import gettext_lazy as _
from django.apps.registry import apps

try:
    apps.check_apps_ready()
except core.exceptions.AppRegistryNotReady:
    import django

    django.setup()

from django_q.brokers import Broker, get_broker
from django_q.conf import Conf, logger, setproctitle
from django_q.models import Success, Task
from django_q.signals import post_execute
from django_q.signing import SignedPackage
from django_tenants_q.utils import QUtilities
from django_q.utils import close_old_django_connections, get_func_repr

from django_tenants.utils import schema_context

try:
    import setproctitle
except ModuleNotFoundError:
    setproctitle = None


def monitor(result_queue: Queue, broker: Broker = None):
    """
    Gets finished tasks from the result queue and saves them to Django
    :type broker: brokers.Broker
    :type result_queue: multiprocessing.Queue
    """
    if not broker:
        broker = get_broker()
    proc_name = current_process().name
    if setproctitle:
        setproctitle.setproctitle(f"qcluster {proc_name} monitor")
    logger.info(
        _("%(name)s monitoring at %(id)s")
        % {"name": proc_name, "id": current_process().pid}
    )
    for task in iter(result_queue.get, "STOP"):
        # save the result
        if task.get("cached", False):
            save_cached(task, broker)
        else:
            save_task(task, broker)
        # acknowledge result
        ack_id = task.pop("ack_id", False)
        if ack_id and (task["success"] or task.get("ack_failure", False)):
            broker.acknowledge(ack_id)
        # signal execution done
        post_execute.send(sender="django_q", task=task)
        # log the result
        info_name = get_func_repr(task["func"])
        if task["success"]:
            # log success
            logger.info(
                _("Processed '%(info_name)s' (%(task_name)s) on %(schema)s")
                % {
                    "info_name": info_name,
                    "task_name": task["name"],
                    "schema": task["kwargs"].get("schema_name", None),
                }
            )
        else:
            # log failure
            logger.error(
                _(
                    "Failed '%(info_name)s' (%(task_name)s) - %(task_result)s on %(schema)s"
                )
                % {
                    "info_name": info_name,
                    "task_name": task["name"],
                    "task_result": task["result"],
                    "schema": task["kwargs"].get("schema_name", None),
                }
            )
    logger.info(_("%(name)s stopped monitoring results") % {"name": proc_name})


def save_task(task, broker: Broker):
    """
    Saves the task package to Django or the cache
    :param task: the task package
    :type broker: brokers.Broker
    """
    # SAVE LIMIT < 0 : Don't save success
    if not task.get("save", Conf.SAVE_LIMIT >= 0) and task["success"]:
        return
    # enqueues next in a chain
    if task.get("chain", None):
        QUtilities.create_async_tasks_chain(
            task["chain"],
            group=task["group"],
            cached=task["cached"],
            sync=task["sync"],
            broker=broker,
        )
    # SAVE LIMIT > 0: Prune database, SAVE_LIMIT 0: No pruning
    close_old_django_connections()

    try:
        kwargs = task.get("kwargs", {})
        schema_name = kwargs.get("schema_name", None)

        if schema_name:
            with schema_context(schema_name):
                if task["success"] and 0 < Conf.SAVE_LIMIT <= Success.objects.count():
                    Success.objects.last().delete()
                # check if this task has previous results

                if Task.objects.filter(id=task["id"], name=task["name"]).exists():
                    existing_task = Task.objects.get(id=task["id"], name=task["name"])
                    # only update the result if it hasn't succeeded yet
                    if not existing_task.success:
                        existing_task.stopped = task["stopped"]
                        existing_task.result = task["result"]
                        existing_task.success = task["success"]
                        existing_task.save()
                else:
                    Task.objects.create(
                        id=task["id"],
                        name=task["name"],
                        func=task["func"],
                        hook=task.get("hook"),
                        args=task["args"],
                        kwargs=task["kwargs"],
                        started=task["started"],
                        stopped=task["stopped"],
                        result=task["result"],
                        group=task.get("group"),
                        success=task["success"],
                    )
        else:
            logger.error("No schema name provided for saving the task")

    except Exception as e:
        logger.error(e)


def save_cached(task, broker):
    task_key = f'{broker.list_key}:{task["id"]}'
    timeout = task["cached"]
    if timeout is True:
        timeout = None
    try:
        group = task.get("group", None)
        iter_count = task.get("iter_count", 0)
        # if it's a group append to the group list
        if group:
            group_key = f"{broker.list_key}:{group}:keys"
            group_list = broker.cache.get(group_key) or []
            # if it's an iter group, check if we are ready
            if iter_count and len(group_list) == iter_count - 1:
                group_args = f"{broker.list_key}:{group}:args"
                # collate the results into a Task result
                results = [
                    SignedPackage.loads(broker.cache.get(k))["result"]
                    for k in group_list
                ]
                results.append(task["result"])
                task["result"] = results
                task["id"] = group
                task["args"] = SignedPackage.loads(broker.cache.get(group_args))
                task.pop("iter_count", None)
                task.pop("group", None)
                if task.get("iter_cached", None):
                    task["cached"] = task.pop("iter_cached", None)
                    save_cached(task, broker=broker)
                else:
                    save_task(task, broker)
                broker.cache.delete_many(group_list)
                broker.cache.delete_many([group_key, group_args])
                return
            # save the group list
            group_list.append(task_key)
            broker.cache.set(group_key, group_list, timeout)
            # async_task next in a chain
            if task.get("chain", None):
                QUtilities.create_async_tasks_chain(
                    task["chain"],
                    group=group,
                    cached=task["cached"],
                    sync=task["sync"],
                    broker=broker,
                )
        # save the task
        broker.cache.set(task_key, SignedPackage.dumps(task), timeout)
    except Exception:
        logger.exception("Could not save task result")
