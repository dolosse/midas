"""
file: midas_kafka_control_test.py
brief: Tests for the Midas control through Kafka module
author: Caswell Pieters
date: 08 May 2020
"""

import pytest
import json
from yaml import safe_load
from confluent_kafka import Producer, KafkaException
import run_control.midas_kafka_control as control
import midas
import midas.client

global yaml_path


@pytest.fixture
def create_client():
    with open(yaml_path) as yf:
        yaml_conf = safe_load(yf)

    client = control.create_midas_client(yaml_conf)
    return client


def test_yaml_verify():
    """Tests the validation of the yaml config file as existing"""

    global yaml_path

    yaml_path = '/home/caswell/dolosse_source/kafka_midas_control/midas_kafka_control.yaml'
    return_path = control.verify_yaml_file(yaml_path)
    assert yaml_path == return_path, 'test failed'


@pytest.mark.skip
def test_create_client():
    with open(yaml_path) as yf:
        yaml_conf = safe_load(yf)

    with pytest.raises(RuntimeError):
        client = control.create_midas_client(yaml_conf)

    client.disconnect()

    with pytest.raises(EnvironmentError):
        client = control.create_midas_client(yaml_conf)

    client.disconnect()


def test_get_value(create_client):
    path = '/Runinfo/asdf/State'
    with pytest.raises(KeyError):
        control.midas_get_value(create_client, path)


def test_midas_execute(create_client):
    msg = {"category": "control", "run": {"action": "Stop", "run_number": 0}}

    with pytest.raises(midas.TransitionFailedError):
        control.midas_execute(msg, create_client)
