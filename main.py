import logging
import RPi.GPIO as GPIO
import time
import yaml

from contextlib import contextmanager
from datetime import datetime
from influxdb import InfluxDBClient
from schematics import Model
from schematics.types import IntType, ModelType, StringType, ListType
from typing import Callable, List


logger = logging.getLogger(__name__)


class ButtonModel(Model):
    # GPIO pin that the button is connected to
    pin = IntType(required=True)  # type: int
    label = StringType(required=True)  # type: str
    value = IntType(required=True)  # type: int


class LedModel(Model):
    # GPIO pin that the LED is connected to
    pin = IntType(required=True)  # type: int
    flash_time_ms = IntType(required=True, default=200)  # type: int


class InfluxdbModel(Model):
    database_name = StringType(required=True)  # type: str
    measurement_name = StringType(required=True, default="pi_mood")  # type: str


class OverallModel(Model):
    buttons = ListType(ModelType(ButtonModel))  # type: List[ButtonModel]
    led = ModelType(LedModel)  # type: LedModel
    # Milliseconds after initial button press registration to ignore any more push events from that button
    bouncetime = IntType(default=200)  # type: int

    influxdb = ModelType(InfluxdbModel)  # type: InfluxdbModel


def load_config_from_file(filename: str) -> OverallModel:
    with open(filename) as fi:
        raw_cfg = yaml.safe_load(fi)

    cfg = OverallModel(raw_cfg)
    cfg.validate(partial=False)
    logger.info(f"Loaded configuration as: {cfg.to_native()}")
    return cfg


def setup_logging() -> None:
    logging.basicConfig(
        format='%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s',
        level=logging.DEBUG,
        datefmt='%Y-%m-%dT%H:%M:%S'
    )


def get_influxdb_client(config: InfluxdbModel) -> InfluxDBClient:
    """
    Get a client connection to the given influxdb database.

    Creates the database if it doesn't exist.

    :return: InfluxDBClient connection.
    """
    influxdb_client = InfluxDBClient(database=config.database_name)

    # Create is a no-op if it already exists.
    influxdb_client.create_database(config.database_name)

    return influxdb_client


def post_to_influxdb_callback(config: InfluxdbModel) -> Callable[[ButtonModel], None]:
    """Upload the given button event to influxdb."""
    influx_client = get_influxdb_client(config)

    def callback(button: ButtonModel) -> None:
        measurement = {
            "measurement": config.measurement_name,
            "fields": {
                button.label: button.value,
            },
            "time": datetime.utcnow(),
        }
        logger.debug(f"Uploading measurement {measurement!r} to influxdb.")
        result = influx_client.write_points([measurement])
        logger.debug(f"Influxdb response: {result}")
    return callback


def flash_led(led: LedModel):
    logger.debug("Turning LED on.")
    GPIO.output(led.pin, GPIO.HIGH)
    time.sleep(led.flash_time_ms / 1000.0)
    GPIO.output(led.pin, GPIO.LOW)
    logger.debug("Turned LED off.")


def callback_for_button(button: ButtonModel, handler: Callable[[ButtonModel], None]) -> Callable[[int], None]:
    def callback(pin: int) -> None:
        logger.info(f"Button {button.label!r} was pressed with args: {pin}")
        if pin != button.pin:
            logger.error(f"Callback for button {button.pin} got event for button {pin}!")
        handler(button)
    return callback


def init_gpio(physical_config: OverallModel, button_handler: Callable) -> None:
    GPIO.setmode(GPIO.BOARD)  # Use physical pin numbering

    # Set each pin to be an input pin and set initial value to be pulled low (off)
    for button in physical_config.buttons:
        GPIO.setup(button.pin, GPIO.IN, pull_up_down=GPIO.PUD_DOWN)
        callback = callback_for_button(button, button_handler)
        GPIO.add_event_detect(button.pin, GPIO.RISING, callback=callback, bouncetime=physical_config.bouncetime)

    GPIO.setup(physical_config.led.pin, GPIO.OUT)
    GPIO.output(physical_config.led.pin, GPIO.LOW)
    logger.info("GPIO init complete.")


def close_gpio() -> None:
    logger.info("Closing GPIO.")
    GPIO.cleanup()


@contextmanager
def gpio_context(physical_config: OverallModel, button_handler: Callable) -> None:
    try:
        init_gpio(physical_config, button_handler)
        yield
    finally:
        close_gpio()


def post_to_influxdb_and_flash_led_callback(config: OverallModel):
    influxdb_callback = post_to_influxdb_callback(config.influxdb)

    def callback(button: ButtonModel) -> None:
        influxdb_callback(button)
        flash_led(config.led)
    return callback


def main(config: OverallModel) -> None:
    # button_callback = post_to_influxdb_callback(config.influxdb)
    button_callback = post_to_influxdb_and_flash_led_callback(config)
    with gpio_context(config, button_callback):
        while True:
            time.sleep(300)
            logger.debug("Still alive...")


if __name__ == "__main__":
    setup_logging()
    config_file_name = "config.yaml"
    cfg = load_config_from_file(config_file_name)
    main(cfg)
