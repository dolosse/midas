"""
file: midas_kafka_control
brief: implementing midas run control through kafka topics, by connecting to the midas ODB
author: Caswell Pieters
date: 18 February 2020
"""

from argparse import ArgumentParser
from json import dumps, loads
import logging
from pathlib import Path
from threading import Thread, Event
from yaml import safe_load

from confluent_kafka import Consumer, Producer, KafkaException
import midas
import midas.client


def midas_execute(json_msg, client):
    """
    Executes the received json command message as a midas run action
    :param json_msg : The json control message.
    :type json_msg : dict
    :param client  : The connected midas client.
    :param client  : The connected midas client.
    :type client : MidasClient
    :return :
    """

    action = json_msg['run']['action']
    run_number = int(json_msg['run']['run_number'])

    run_actions = {
        'Start': 'client.start_run(run_number)',
        'Stop': 'client.stop_run()',
        'Pause': 'client.pause_run()',
        'Resume': 'client.resume_run()'
    }

    try:
        exec(run_actions[action])
    except midas.TransitionFailedError as trans_err:
        print(trans_err)


def delivery_callback(err, msg):
    """
    Reports the failure or success of a message delivery.
    :param err : The error that occurred on None or success.
    :type err : KafkaError
    :param msg  : The message that was produced or failed.
    :type msg : Message
    :return :
    """

    if err:
        logging.error('%% Message failed delivery: %s\n' % err)
    else:
        logging.info('%% Message delivered to %s [%d] @ %d\n' %
                     (msg.topic(), msg.partition(), msg.offset()))


def key_capture_thread(exit_event):
    """
    Captures quit and Enter to exit the program.
    :param exit_event : The exit Event.
    :type exit_event : Event
    :return :
    """

    while not exit_event.isSet():
        if input() == 'quit':
            exit_event.set()


def midas_get_value(client, path):
    """
    Return the value of the Midas odb entry at the given path
    :param client: MidasClient
    :param path: str
    :return:
    """
    value = ' '
    try:
        value = client.odb_get(path, True, False)
    except KeyError:
        logging.error("Path does not exist in Midas", exc_info=True)
        raise
    except midas.MidasError:
        logging.error("Midas Error", exc_info=True)
        raise

    return value


def midas_info(t_feedback, t_errors, midas_equipment, producer, client):
    """
    This function gets stats and error information from the Midas odb and produces them to kafka.
    :param t_feedback  : Feedback topic name.
    :type t_feedback : str
    :param t_errors  : Errors topic name.
    :type t_errors : str
    :param midas_equipment  : The Midas equipment name from the DAQ frontend.
    :type midas_equipment : str
    :param producer  : The Kafka producer
    :type producer : Producer
    :param client : The connected Midas client
    :type client : MidasClient
    :raises KafkaException : Producer error
    :raises BufferError : Producer error
    :return :
    """

    # query the MIDAS odb for status information
    daq_states = ['Stopped', 'Paused', 'Running']
    run_state = midas_get_value(client, '/Runinfo/State')
    run_number = midas_get_value(client, '/Runinfo/Run number')
    events = midas_get_value(client, '/Equipment/' + midas_equipment + '/Statistics/Events sent')
    evt_rate = midas_get_value(client, '/Equipment/' + midas_equipment + '/Statistics/Events per sec.')
    kb_rate = midas_get_value(client, '/Equipment/' + midas_equipment + '/Statistics/kBytes per sec.')
    error = 'test error'
    feedback = 'test feedback'

    # compose feedback message
    feedback_json = dumps({"category": "feedback", "daq": {"run_state": daq_states[run_state - 1],
                                                           "evt_rate": evt_rate,
                                                           "kB_rate": kb_rate,
                                                           "events": events}, "os": feedback})

    # compose error message
    error_json = dumps({"category": "errors", "msg": error})

    try:
        # Produce feedback json message
        producer.produce(t_feedback, feedback_json, callback=delivery_callback)
    except BufferError:
        logging.info('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                     len(producer))
    except KafkaException:
        logging.error("Kafka Exception occurred", exc_info=True)

    try:
        # Produce error json message
        producer.produce(t_errors, error_json, callback=delivery_callback)
    except BufferError:
        logging.info('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                     len(producer))
    except KafkaException:
        logging.error("Kafka Exception occurred", exc_info=True)


def verify_yaml_file(arg_config):
    """
    Checks whether the path to the yaml file points to an existing file and exits if not.
    :param arg_config : The command line argument path to the yaml config file
    :type arg_config : str
    :return : The absolute path to the yaml config file
    :rtype  : str
    """

    if not Path(arg_config).exists():
        print('Yaml config file does not exist')
        exit(1)
    else:
        return arg_config


def get_yaml_file():
    """
    Gets the yaml file from the first command line input parameter.
    :return : The absolute path to the yaml config file
    :rtype  : str
    """

    # parse command line argument for config yaml file
    ap = ArgumentParser()
    ap.add_argument("--yaml-config", required=True, help='The absolute path to the yaml config file')
    args = ap.parse_args()
    return verify_yaml_file(args.yaml_config)


def create_midas_client(yaml_conf):
    """
    Create the Midas client.

    Create the Midas client
    :raises RuntimeError : Only one client at a time can be connected
    :raises EnvironmentError : $MIDASSYS not set
    :return : The connected Midas client
    :rtype :  MidasClient
    """
    try:
        client = midas.client.MidasClient("kafka_control", host_name=yaml_conf['kafka']['expt_host'],
                                          expt_name=yaml_conf['kafka']['expt_name'])
    except RuntimeError as run_err:
        print(run_err)
    except EnvironmentError as env_err:
        print(env_err)

    return client


def initialize(yaml_conf):
    """
    Initializing objects.

    Creates the logger for Kafka message reports, and creates the Kafka producer and consumer and Midas client
    :raises KafkaException : Consumer could not subscribe to topics
    :raises RuntimeError : Consumer could not subscribe to topics
    :return : A tuple of the producer and consumer
    :rtype : Producer, Consumer, MidasClient
    """

    # create logger
    logging.basicConfig(level=logging.DEBUG, filename='kafka_interface.log', filemode='w',
                        format='%(name)s - %(levelname)s - %(message)s')

    # consumer configuration
    kafka_conf = {'bootstrap.servers': yaml_conf['kafka']['bootstrap_servers'],
                  'group.id': yaml_conf['kafka']['group_id'], 'auto.offset.reset': 'latest'}

    # producer configuration
    kafka_conf_prod = {'bootstrap.servers': yaml_conf['kafka']['bootstrap_servers']}

    # create consumer
    consumer = Consumer(kafka_conf)
    try:
        consumer.subscribe([yaml_conf['kafka']['topics']['control']])
    except KafkaException:
        print('Kafka Error in subscribing to consumer topics')
        exit(1)
    except RuntimeError:
        print('Could not subscribe to consumer topics - Consumer closed')
        exit(1)

    # create producer
    producer = Producer(**kafka_conf_prod)

    # make the midas client
    client = create_midas_client(yaml_conf)

    return producer, consumer, client


def main():
    """
    The main function for midas kafka control.

    Get the yaml config, get midas odb info and consume control commands.
    :return :
    """

    # get the yaml config
    yaml_file = get_yaml_file()

    # read yaml file
    with open(yaml_file) as yf:
        yaml_conf = safe_load(yf)

    # initialize
    init_values = initialize(yaml_conf)
    control_producer = init_values[0]
    control_consumer = init_values[1]
    midas_client = init_values[2]

    # e thread exit event
    e = Event()

    # start exit event capture thread
    Thread(target=key_capture_thread, args=(e,), name='key_capture_thread',
           daemon=True).start()

    print('Type quit and Enter to exit')

    while not e.isSet():
        midas_info(yaml_conf['kafka']['topics']['feedback'], yaml_conf['kafka']['topics']['errors'],
                   yaml_conf['kafka']['midas_equipment'], control_producer, midas_client)
        msg = control_consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            logging.error("Kafka Exception occurred", exc_info=True)
            raise KafkaException(msg.error())
        else:
            # Proper message
            logging.info('%% %s [%d] at offset %d with key %s:\n' %
                         (msg.topic(), msg.partition(), msg.offset(),
                          str(msg.key())))
            json_data = loads(msg.value())
            midas_execute(json_data, midas_client)

    # Close down consumer to commit final offsets.
    control_consumer.close()
    # Wait for messages to be delivered
    control_producer.flush()
    # disconnect from midas experiment
    midas_client.disconnect()


if __name__ == "__main__":
    main()
