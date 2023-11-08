from dagster_slack import slack_resource
from dagster import failure_hook, success_hook
from dagster import HookContext
from ..resources.config import SLACK_DEFAULT_CHANNEL

@success_hook(required_resource_keys={"slack"})
def slack_message_on_success(context: HookContext):
    message = f"Op {context.op.name} finished successfully"
    context.resources.slack.chat_postMessage(channel=SLACK_DEFAULT_CHANNEL, text=message)


@failure_hook(required_resource_keys={"slack"})
def slack_message_on_failure(context: HookContext):
    message = f"Op {context.op.name} failed"
    context.resources.slack.chat_postMessage(channel=SLACK_DEFAULT_CHANNEL, text=message)


@success_hook(required_resource_keys={"healthchecks"})
def notify_healthchecks_on_success(context):
    context.resources.healthchecks.send_update()


@failure_hook(required_resource_keys={"healthchecks"})
def notify_healthchecks_on_failure(context):
    context.resources.healthchecks.send_update("fail")

HOOKS = {
	slack_message_on_failure,
	slack_message_on_success,
	notify_healthchecks_on_failure,
	notify_healthchecks_on_success,
}
