import logging
import warnings
from typing import Any, Dict, Text, Optional, Union

from rasa.utils import common
from rasa.utils.endpoints import EndpointConfig

logger = logging.getLogger(__name__)


class EventBroker:
    @staticmethod
    def create(
        obj: Union["EventBroker", EndpointConfig, None],
    ) -> Optional["EventBroker"]:
        """Factory to create an event broker."""

        if isinstance(obj, EventBroker):
            return obj
        else:
            return _create_from_endpoint_config(obj)

    @classmethod
    def from_endpoint_config(cls, broker_config: EndpointConfig) -> "EventBroker":
        raise NotImplementedError(
            "Event broker must implement the `from_endpoint_config` method."
        )

    def publish(self, event: Dict[Text, Any]) -> None:
        """Publishes a json-formatted Rasa Core event into an event queue."""

        raise NotImplementedError("Event broker must implement the `publish` method.")


def _create_from_endpoint_config(
    endpoint_config: Optional[EndpointConfig],
) -> Optional["EventBroker"]:
    """Instantiate an event broker based on its configuration."""

    if endpoint_config is None:
        return None
    elif endpoint_config.type is None or endpoint_config.type.lower() == "pika":
        from rasa.core.brokers.pika import PikaEventBroker

        return PikaEventBroker.from_endpoint_config(endpoint_config)
    elif endpoint_config.type.lower() == "sql":
        from rasa.core.brokers.sql import SQLEventBroker

        return SQLEventBroker.from_endpoint_config(endpoint_config)
    elif endpoint_config.type.lower() == "file":
        from rasa.core.brokers.file import FileEventBroker

        return FileEventBroker.from_endpoint_config(endpoint_config)
    elif endpoint_config.type.lower() == "kafka":
        from rasa.core.brokers.kafka import KafkaEventBroker

        return KafkaEventBroker.from_endpoint_config(endpoint_config)
    else:
        return _load_from_module_string(endpoint_config)


def _load_from_module_string(broker_config: EndpointConfig,) -> Optional["EventBroker"]:
    """Instantiate an event broker based on its class name."""

    try:
        event_broker_class = common.class_from_module_path(broker_config.type)
        return event_broker_class.from_endpoint_config(broker_config)
    except (AttributeError, ImportError) as e:
        logger.warning(
            f"The `EventBroker` type '{broker_config.type}' could not be found. "
            f"Not using any event broker. Error: {e}"
        )
        return None
