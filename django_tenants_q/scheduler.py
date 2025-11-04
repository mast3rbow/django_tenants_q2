import ast
from multiprocessing.process import current_process

from django import core, db
from django.utils import timezone
from django.utils.translation import gettext_lazy as _

from django.apps.registry import apps

try:
    apps.check_apps_ready()
except core.exceptions.AppRegistryNotReady:
    import django

    django.setup()

from django.conf import settings

from django_q.brokers import Broker, get_broker
from django_q.conf import Conf, logger
from django_q.humanhash import humanize
from django_q.models import Schedule
from django_tenants_q.utils import QUtilities
from django_q.utils import close_old_django_connections, localtime

from django_tenants.utils import schema_context, get_tenant_model


def scheduler(broker=None):
    """
    Creates a task from a schedule at the scheduled time and schedules next run
    """
    if not broker:
        broker = get_broker()
    close_old_django_connections()
    tenant_model = get_tenant_model()
    django_tenants_to_exclude = getattr(
        settings, "SCHEMAS_TO_BE_EXCLUDED_BY_SCHEDULER", ["public"]
    )

    try:
        tenants_qs = tenant_model.objects.exclude(
            schema_name__in=django_tenants_to_exclude
        )
        # If the tenant model has an `is_active` flag, only include active tenants.
        try:
            tenant_model._meta.get_field("is_active")
        except Exception:
            # Field doesn't exist -> keep original queryset
            pass
        else:
            tenants_qs = tenants_qs.filter(is_active=True)

        for tenant in tenants_qs:
            with schema_context(tenant.schema_name):
                # Only default cluster will handler schedule with default(null) cluster
                Q_default = (
                    db.models.Q(cluster__isnull=True)
                    if Conf.CLUSTER_NAME == Conf.PREFIX
                    else db.models.Q(pk__in=[])
                )

                with db.transaction.atomic(using=db.router.db_for_write(Schedule)):
                    for s in (
                        Schedule.objects.select_for_update()
                        .exclude(repeats=0)
                        .filter(next_run__lt=timezone.now())
                        .filter(Q_default | db.models.Q(cluster=Conf.CLUSTER_NAME))
                    ):
                        args = ()
                        kwargs = {}
                        # get args, kwargs and hook
                        if s.kwargs:
                            try:
                                # eval should be safe here because dict()
                                kwargs = ast.literal_eval(s.kwargs)
                            except (SyntaxError, ValueError):
                                try:
                                    parsed_kwargs = (
                                        ast.parse(f"f({s.kwargs})")
                                        .body[0]
                                        .value.keywords
                                    )
                                    kwargs = {
                                        kwarg.arg: ast.literal_eval(kwarg.value)
                                        for kwarg in parsed_kwargs
                                    }
                                except (SyntaxError, ValueError):
                                    kwargs = {}
                        if s.args:
                            args = ast.literal_eval(s.args)
                            # single value won't eval to tuple, so:
                            if type(args) != tuple:
                                args = (args,)
                        q_options = kwargs.get("q_options", {})
                        if s.intended_date_kwarg:
                            kwargs[s.intended_date_kwarg] = s.next_run.isoformat()
                        if s.hook:
                            q_options["hook"] = s.hook
                        # set up the next run time (only for non-ONCE schedules)
                        if s.schedule_type != s.ONCE:
                            next_run = s.next_run
                            while True:
                                next_run = s.calculate_next_run(next_run)
                                if Conf.CATCH_UP or next_run > localtime():
                                    break
                            s.next_run = next_run
                            # Little fix for already broken numbers
                            if s.repeats < -1:
                                s.repeats = -1
                            # decrement repeats only when positive
                            if s.repeats > 0:
                                s.repeats -= 1

                        # send it to the cluster; any cluster name is allowed in multi-queue scenarios
                        # because `broker_name` is confusing, using `cluster` name is recommended and takes precedence
                        q_options["cluster"] = s.cluster or q_options.get(
                            "cluster", q_options.pop("broker_name", None)
                        )
                        if (
                            q_options["cluster"] is None
                            or q_options["cluster"] == Conf.CLUSTER_NAME
                        ):
                            q_options["broker"] = broker
                        q_options["group"] = q_options.get("group", s.name or s.id)
                        kwargs["q_options"] = q_options
                        s.task = QUtilities.add_async_task(s.func, *args, **kwargs)
                        # log it
                        if not s.task:
                            logger.error(
                                _(
                                    "%(process_name)s failed to create a task from schedule for %(tenant)s "
                                    "[%(schedule)s]"
                                )
                                % {
                                    "process_name": current_process().name,
                                    "schedule": s.name or s.id,
                                    "tenant": tenant.schema_name,
                                }
                            )
                        else:
                            logger.info(
                                _(
                                    "%(process_name)s created task %(task_name)s from schedule for %(tenant)s "
                                    "[%(schedule)s]"
                                )
                                % {
                                    "process_name": current_process().name,
                                    "task_name": humanize(s.task),
                                    "schedule": s.name or s.id,
                                    "tenant": tenant.schema_name,
                                }
                            )
                        # default behavior is to delete a ONCE schedule
                        if s.schedule_type == s.ONCE:
                            if s.repeats < 0:
                                s.delete()
                                continue
                            # but not if it has a positive repeats
                            s.repeats = 0
                        # save the schedule
                        s.save()
    except Exception:
        logger.exception("Could not create task from schedule")
