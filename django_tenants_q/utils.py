# standard
from multiprocessing import Value

# django
from django.db import connection
from django.utils import timezone

# local
from django_q.queues import Queue
from django_q.humanhash import uuid
from django_q.conf import Conf, logger
from django_q.brokers import get_broker
from django_q.signals import pre_enqueue
from django_q.signing import SignedPackage
from django_tenants.utils import schema_context
from django_q.tasks import (
    schedule,
    result,
    result_group,
    fetch,
    fetch_group,
    count_group,
    delete_group,
    delete_cached,
    queue_size,
)


class QUtilities(object):

    @staticmethod
    def prepare_task(func, *args, **kwargs):
        keywords = kwargs.copy()
        opt_keys = (
            "hook",
            "group",
            "save",
            "sync",
            "cached",
            "ack_failure",
            "iter_count",
            "iter_cached",
            "chain",
            "broker",
            "timeout",
        )
        q_options = keywords.pop("q_options", {})
        # get an id
        tag = uuid()
        # build the task package
        task = {
            "id": tag[1],
            "name": keywords.pop("task_name", None)
            or q_options.pop("task_name", None)
            or tag[0],
            "func": func,
            "args": args,
        }
        # push optionals
        for key in opt_keys:
            if q_options and key in q_options:
                task[key] = q_options[key]
            elif key in keywords:
                task[key] = keywords.pop(key)
        # don't serialize the broker
        broker = task.pop("broker", get_broker())
        # overrides
        if "cached" not in task and Conf.CACHED:
            task["cached"] = Conf.CACHED
        if "sync" not in task and Conf.SYNC:
            task["sync"] = Conf.SYNC
        if "ack_failure" not in task and Conf.ACK_FAILURES:
            task["ack_failure"] = Conf.ACK_FAILURES
        # finalize
        task["kwargs"] = keywords
        task["started"] = timezone.now()
        # signal it
        pre_enqueue.send(sender="django_q", task=task)
        # sign it
        pack = SignedPackage.dumps(task)
        return tag, task, broker, pack

    @staticmethod
    def add_async_task(func, *args, **kwargs):
        # Wrapper method to add a task with awareness of schemapack
        if "schema_name" not in kwargs:
            kwargs.update({"schema_name": connection.schema_name})
        tag, task, broker, pack = QUtilities.prepare_task(func, *args, **kwargs)
        if task.get("sync", False):
            return QUtilities.run_synchronously(pack)
        enqueue_id = broker.enqueue(pack)
        logger.info(f"Enqueued {enqueue_id}")
        logger.debug(f"Pushed {tag}")
        return task["id"]

    @staticmethod
    def create_schedule(func, *args, **kwargs):
        # Wrapper method to create schedule with awareness of schema
        schema_name = kwargs.get("schema_name", connection.schema_name)
        if schema_name:
            with schema_context(schema_name):
                new_schedule = schedule(func, *args, **kwargs)
                new_schedule.save()
                return new_schedule
        else:
            logger.error("No schema name was provided")
            return None

    @staticmethod
    def get_result(task_id, wait=0, cached=Conf.CACHED):
        # Wrapper method to get result of a task with awareness of schema
        schema_name = connection.schema_name
        with schema_context(schema_name):
            return result(task_id, wait, cached)

    @staticmethod
    def get_result_group(
        group_id, failures=False, wait=0, count=None, cached=Conf.CACHED
    ):
        # Wrapper method to get result of a group with awareness of schema
        schema_name = connection.schema_name
        with schema_context(schema_name):
            return result_group(group_id, failures, wait, count, cached=cached)

    @staticmethod
    def fetch_task(task_id, wait=0, cached=Conf.CACHED):
        # Wrapper method to fetch a single task with awareness of schema
        schema_name = connection.schema_name
        with schema_context(schema_name):
            return fetch(task_id, wait, cached)

    @staticmethod
    def fetch_task_group(
        group_id, failures=True, wait=0, count=None, cached=Conf.CACHED
    ):
        # Wrapper method to get a group with tasks with awareness of schema
        schema_name = connection.schema_name
        with schema_context(schema_name):
            return fetch_group(group_id, failures, wait, count, cached=cached)

    @staticmethod
    def get_group_count(group_id, failures=False, cached=Conf.CACHED):
        # Wrapper method to get count of groups with awareness of schema
        schema_name = connection.schema_name
        with schema_context(schema_name):
            return count_group(group_id, failures, cached)

    @staticmethod
    def delete_task_group(group_id, tasks=False, cached=Conf.CACHED):
        # Wrapper method to delete task group with awareness of schema
        schema_name = connection.schema_name
        with schema_context(schema_name):
            return delete_group(group_id, tasks, cached)

    @staticmethod
    def delete_task_from_cache(task_id, broker=None):
        # Wrapper method to delete a task from the cache
        return delete_cached(task_id, broker=broker)

    @staticmethod
    def get_queue_size(broker=None):
        # Wrapper method to get the queue size
        return queue_size(broker)

    @staticmethod
    def add_async_tasks_from_iter(func, args_iter, **kwargs):
        """
        Wrapper around async_iter that enqueues a function with iterable arguments
        """
        iter_count = len(args_iter)
        iter_group = uuid()[1]

        # clean up the kwargs
        options = kwargs.get("q_options", kwargs)
        options.pop("hook", None)
        options["broker"] = options.get("broker", get_broker())
        options["group"] = iter_group
        options["iter_count"] = iter_count
        if options.get("cached", None):
            options["iter_cached"] = options["cached"]
        options["cached"] = True
        # save the original arguments
        broker = options["broker"]
        broker.cache.set(
            f"{broker.list_key}:{iter_group}:args", SignedPackage.dumps(args_iter)
        )

        for args in args_iter:
            if not isinstance(args, tuple):
                args = (args,)
            QUtilities.add_async_task(func, *args, **options)
        return iter_group

    @staticmethod
    def create_async_tasks_chain(
        chain, group=None, cached=Conf.CACHED, sync=Conf.SYNC, broker=None
    ):
        """
        Wrapper method around async_chain that enqueues a chain of tasks
        the chain must be in the format [(func,(args),{kwargs}),(func,(args),{kwargs})]
        """
        if not group:
            group = uuid()[1]
        args = ()
        kwargs = {}
        task = chain.pop(0)
        if type(task) is not tuple:
            task = (task,)
        if len(task) > 1:
            args = task[1]
        if len(task) > 2:
            kwargs = task[2]
        kwargs["chain"] = chain
        kwargs["group"] = group
        kwargs["cached"] = cached
        kwargs["sync"] = sync
        kwargs["broker"] = broker or get_broker()
        QUtilities.add_async_task(task[0], *args, **kwargs)
        return group

    @staticmethod
    def run_synchronously(pack):
        """Method to run a task synchoronously"""

        from django_tenants_q.cluster import worker, monitor

        task_queue = Queue()
        result_queue = Queue()
        task = SignedPackage.loads(pack)
        task_queue.put(task)
        task_queue.put("STOP")
        worker(task_queue, result_queue, Value("f", -1))
        result_queue.put("STOP")
        monitor(result_queue)
        task_queue.close()
        task_queue.join_thread()
        result_queue.close()
        result_queue.join_thread()
        return task["id"]
