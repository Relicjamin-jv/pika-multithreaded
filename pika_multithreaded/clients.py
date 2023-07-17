import functools
import logging
import pika
from pika.exceptions import ChannelClosedByBroker, StreamLostError, ChannelWrongStateError
import signal
import ssl
import threading
import uuid

from .utils import AmqpUtils


class AmqpClient:
    def __init__(self, host=None, port=None, user=None, password=None, use_ssl=False, url=None):
        if url:
            self.url = url
        else:
            self.url = AmqpUtils.generate_url(
                host, port, user, password, use_ssl)
        # Default the connection info to None to signal a connection has not been made yet
        self._clear_connection()
        self.consumer_tag = None
        self.logger = logging.getLogger(__name__)

    def __enter__(self):
        """
        This allows the use of the "with AmqpClient:" syntax so that it will
        autoclose the connection when the block is done executing.
        """
        if not self.connection:
            self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        This allows the use of the "with AmqpClient:" syntax so that it will
        autoclose the connection when the block is done executing.
        """
        self.close()

    @property
    def _is_connection_alive(self):
        return self.connection and self.connection.is_open

    def _clear_connection(self):
        self.connection = None
        self.channel = None

    def setup_signal_handlers(self):
        self.logger.debug("Setting up SIGINT/SIGTERM handler...")
        # Set up signal handlers since this client is intended to be run as its own process
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def queue_declare(self, queue_name, durable=False):
        # Keep track whether or not we need to auto-close the connection after we're done
        auto_close_connection = False
        if not self.connection:
            self.connect()
            auto_close_connection = True
        self.channel.queue_declare(queue_name, durable=durable)
        # Close the connection if we opened it at the beginning of this function
        if auto_close_connection:
            self.close()

    def connect(self):
        if self.url.startswith("amqps://"):
            # Looks like we're making a secure connection
            # Create the SSL context for our secure connection. This context forces a more secure
            # TLSv1.2 connection and uses FIPS compliant cipher suites. To understand what suites
            # we're using here, read docs on OpenSSL cipher list format:
            # https://www.openssl.org/docs/man1.1.1/man1/ciphers.html
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
            ssl_context.set_ciphers('ECDHE+AESGCM:!ECDSA')
            # Create the URL parameters for our connection
            url_parameters = pika.URLParameters(self.url)
            url_parameters.ssl_options = pika.SSLOptions(context=ssl_context)
        elif self.url.startswith("amqp://"):
            # Looks like we're making a clear-text connection
            # Create the connection and store them in self
            url_parameters = pika.URLParameters(self.url)
        else:
            raise Exception("AMQP URL must start with 'amqp://' or 'amqps://'")

        # Create the connection and store them in self
        self.connection = pika.BlockingConnection(url_parameters)
        self.channel = self.connection.channel()

    def _reconnect_channel(self):
        if self._is_connection_alive:
            try:
                self.channel.close()
            except Exception:
                pass
            self.channel = self.connection.channel()
        else:
            try:
                self.close()
            except Exception:
                pass
            self.connect()

    def close(self):
        # Stop consuming if we've started consuming already
        if self.consumer_tag:
            self.stop_consuming()
        # Close the connection
        if self._is_connection_alive:
            self.connection.close()
            self._clear_connection()
        elif self.connection:
            self._clear_connection()

    def send_message(self, routing_key, message, exchange=None):
        # Keep track whether or not we need to auto-close the connection after we're done
        auto_close_connection = False
        if not self.connection:
            self.connect()
            auto_close_connection = True
        # Set the exchange to the default (empty string) if none was supplied
        if not exchange:
            exchange = ''
        # Publish a message to the correct location
        self.channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=message
        )
        # Close the connection if we opened it at the beginning of this function
        if auto_close_connection:
            self.close()

    def get_message(self, queue):
        # Keep track whether or not we need to auto-close the connection after we're done
        auto_close_connection = False
        if not self.connection:
            self.connect()
            auto_close_connection = True
        # Attempt to get a message from the server
        method_frame, header_frame, body = self.channel.basic_get(queue)
        # If we get something back, ACK the message and return it. If not, return a bunch of Nuns.
        # (ha! get it?)
        if method_frame:
            self.channel.basic_ack(method_frame.delivery_tag)
        # Close the connection if we opened it at the beginning of this function
        if auto_close_connection:
            self.close()
        return method_frame, header_frame, body

    def ack_message(self, delivery_tag):
        # Keep track whether or not we need to auto-close the connection after we're done
        auto_close_connection = False
        if not self.connection:
            self.connect()
            auto_close_connection = True
        # Publish a message to the correct location
        self.channel.basic_ack(delivery_tag=delivery_tag)
        # Close the connection if we opened it at the beginning of this function
        if auto_close_connection:
            self.close()

    def ack_message_threadsafe(self, delivery_tag):
        if self.connection:
            self.connection.add_callback_threadsafe(
                functools.partial(self.ack_message, delivery_tag)
            )

    def nack_message(self, delivery_tag, requeue=True):
        # Keep track whether or not we need to auto-close the connection after we're done
        auto_close_connection = False
        if not self.connection:
            self.connect()
            auto_close_connection = True
        # Publish a message to the correct location
        self.channel.basic_nack(delivery_tag=delivery_tag, requeue=requeue)
        # Close the connection if we opened it at the beginning of this function
        if auto_close_connection:
            self.close()

    def nack_message_threadsafe(self, delivery_tag, requeue=True):
        if self.connection:
            self.connection.add_callback_threadsafe(
                functools.partial(self.nack_message, delivery_tag, requeue)
            )

    def consume(
            self, queue, callback_function,
            auto_ack=False, consumer_tag=None, declare_queue=True, qos_count=1):
        # Keep track whether or not we need to auto-close the connection after we're done
        auto_close_connection = False
        if not self.connection:
            self.connect()
            auto_close_connection = True
        # =============================
        if declare_queue:
            self.queue_declare(queue, durable=True)
        keep_consuming = True
        self.user_consumer_callback = callback_function
        while keep_consuming:
            self.logger.debug(f"Connecting to queue {queue}...")
            try:
                # Set QOS prefetch count. Now that this is multi-threaded, we can now control how
                # many messages we process in parallel by simply increasing this number.
                self.channel.basic_qos(prefetch_count=qos_count)
                # Consume the queue
                if consumer_tag:
                    self.consumer_tag = consumer_tag
                else:
                    self.consumer_tag = f"pika-amqp-client-{str(uuid.uuid4())}"
                print(f"{'*'*20}\nDefining consumer...\n{'*'*20}")
                self.channel.basic_consume(
                    queue,
                    self._consumer_callback,
                    auto_ack=auto_ack,
                    consumer_tag=self.consumer_tag)
                print(f"{'*'*20}\nStarting consumer...\n{'*'*20}")
                self.channel.start_consuming()
                print(f"{'*'*20}\nExiting consumer...\n{'*'*20}")
                keep_consuming = False
            except (StreamLostError, ChannelClosedByBroker) as ex:
                # There is a timeout of 1800000 ms that results in this exception so catch the
                # exception and re-start the consumer
                self.logger.error(
                    f"Connection Error: {ex}",
                    exc_info=True)
                keep_consuming = True
            except ChannelWrongStateError as ex:
                self.logger.error(
                    f"Channel Error: Possible usage of already closed channel --> {ex}",
                    exc_info=True)
                self.stop_consuming()
                self._reconnect_channel()
                keep_consuming = True
            except Exception as ex:
                self.logger.error(
                    f"FATAL ERROR: {ex}",
                    exc_info=True)
                self.logger.debug("General exception. Closing consumer...")
                self.stop_consuming()
                keep_consuming = False
        # =============================
        # Close the connection if we opened it at the beginning of this function
        if auto_close_connection:
            self.close()

    def _consumer_callback(self, channel, method, properties, body):
        new_thread = threading.Thread(
            target=self.user_consumer_callback,
            args=[self, channel, method, properties, body],
            daemon=True
        )
        new_thread.start()

    def _signal_handler(self, sig, frame):
        self.logger.warning(
            "*** AMQP Client terminating. Closing AMQP connection...")
        print(f"{'*'*20}\nSIGINT/SIGTERM handler\n{'*'*20}")
        print(f"{'*'*20}\nStopping Consumer...\n{'*'*20}")
        self.stop_consuming()
        print(f"{'*'*20}\nClosing AMQP connection...\n{'*'*20}")
        self.close()

    def stop_consuming(self):
        if self.consumer_tag and self._is_connection_alive and self.channel.is_open:
            self.logger.debug(f"{'*'*20}\nClosing consumer...\n{'*'*20}")
            self.channel.basic_cancel(self.consumer_tag)
            self.consumer_tag = None
            self.user_consumer_callback = None


class AmqpClientAsync:
    """This is an example consumer that will handle unexpected interactions
    with RabbitMQ such as channel and connection closures.

    If RabbitMQ closes the connection, this class will stop and indicate
    that reconnection is necessary. You should look at the output, as
    there are limited reasons why the connection may be closed, which
    usually are tied to permission related issues or socket timeouts.

    If the channel is closed, it will indicate a problem with one of the
    commands that were issued and that should surface in the output as well.

    """
    EXCHANGE = 'message'
    QUEUE = 'test'
    ROUTING_KEY = 'example.test'

    def __init__(self, amqp_url):
        """Create a new instance of the consumer class, passing in the AMQP
        URL used to connect to RabbitMQ.

        :param str amqp_url: The AMQP url to connect with

        """
        self.should_reconnect = False
        self.was_consuming = False

        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self._url = amqp_url
        self._consuming = False
        # In production, experiment with higher prefetch values
        # for higher consumer throughput
        self._prefetch_count = 1
        self.logger = logging.getLogger(__name__)

    def connect(self):
        """This method connects to RabbitMQ, returning the connection handle.
        When the connection is established, the on_connection_open method
        will be invoked by pika.

        :rtype: pika.SelectConnection

        """
        LOGGER.info('Connecting to %s', self._url)
        return pika.SelectConnection(
            parameters=pika.URLParameters(self._url),
            on_open_callback=self.on_connection_open,
            on_open_error_callback=self.on_connection_open_error,
            on_close_callback=self.on_connection_closed)

    def close_connection(self):
        self._consuming = False
        if self._connection.is_closing or self._connection.is_closed:
            LOGGER.info('Connection is closing or already closed')
        else:
            LOGGER.info('Closing connection')
            self._connection.close()

    def on_connection_open(self, _unused_connection):
        """This method is called by pika once the connection to RabbitMQ has
        been established. It passes the handle to the connection object in
        case we need it, but in this case, we'll just mark it unused.

        :param pika.SelectConnection _unused_connection: The connection

        """
        LOGGER.info('Connection opened')
        self.open_channel()

    def on_connection_open_error(self, _unused_connection, err):
        """This method is called by pika if the connection to RabbitMQ
        can't be established.

        :param pika.SelectConnection _unused_connection: The connection
        :param Exception err: The error

        """
        LOGGER.error('Connection open failed: %s', err)
        self.reconnect()

    def on_connection_closed(self, _unused_connection, reason):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.

        :param pika.connection.Connection connection: The closed connection obj
        :param Exception reason: exception representing reason for loss of
            connection.

        """
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            LOGGER.warning(
                'Connection closed, reconnect necessary: %s', reason)
            self.reconnect()

    def reconnect(self):
        """Will be invoked if the connection can't be opened or is
        closed. Indicates that a reconnect is necessary then stops the
        ioloop.

        """
        self.should_reconnect = True
        self.stop()

    def open_channel(self):
        """Open a new channel with RabbitMQ by issuing the Channel.Open RPC
        command. When RabbitMQ responds that the channel is open, the
        on_channel_open callback will be invoked by pika.

        """
        LOGGER.info('Creating a new channel')
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        """This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.

        Since the channel is now open, we'll declare the exchange to use.

        :param pika.channel.Channel channel: The channel object

        """
        LOGGER.info('Channel opened')
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange(self.EXCHANGE)

    def add_on_channel_close_callback(self):
        """This method tells pika to call the on_channel_closed method if
        RabbitMQ unexpectedly closes the channel.

        """
        LOGGER.info('Adding channel close callback')
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reason):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.

        :param pika.channel.Channel: The closed channel
        :param Exception reason: why the channel was closed

        """
        LOGGER.warning('Channel %i was closed: %s', channel, reason)
        self.close_connection()

    def setup_exchange(self, exchange_name):
        """Setup the exchange on RabbitMQ by invoking the Exchange.Declare RPC
        command. When it is complete, the on_exchange_declareok method will
        be invoked by pika.

        :param str|unicode exchange_name: The name of the exchange to declare

        """
        LOGGER.info('Declaring exchange: %s', exchange_name)
        # Note: using functools.partial is not required, it is demonstrating
        # how arbitrary data can be passed to the callback when it is called
        cb = functools.partial(
            self.on_exchange_declareok, userdata=exchange_name)
        self._channel.exchange_declare(
            exchange=exchange_name,
            exchange_type=self.EXCHANGE_TYPE,
            callback=cb)

    def on_exchange_declareok(self, _unused_frame, userdata):
        """Invoked by pika when RabbitMQ has finished the Exchange.Declare RPC
        command.

        :param pika.Frame.Method unused_frame: Exchange.DeclareOk response frame
        :param str|unicode userdata: Extra user data (exchange name)

        """
        LOGGER.info('Exchange declared: %s', userdata)
        self.setup_queue(self.QUEUE)

    def setup_queue(self, queue_name):
        """Setup the queue on RabbitMQ by invoking the Queue.Declare RPC
        command. When it is complete, the on_queue_declareok method will
        be invoked by pika.

        :param str|unicode queue_name: The name of the queue to declare.

        """
        LOGGER.info('Declaring queue %s', queue_name)
        cb = functools.partial(self.on_queue_declareok, userdata=queue_name)
        self._channel.queue_declare(queue=queue_name, callback=cb)

    def on_queue_declareok(self, _unused_frame, userdata):
        """Method invoked by pika when the Queue.Declare RPC call made in
        setup_queue has completed. In this method we will bind the queue
        and exchange together with the routing key by issuing the Queue.Bind
        RPC command. When this command is complete, the on_bindok method will
        be invoked by pika.

        :param pika.frame.Method _unused_frame: The Queue.DeclareOk frame
        :param str|unicode userdata: Extra user data (queue name)

        """
        queue_name = userdata
        LOGGER.info('Binding %s to %s with %s', self.EXCHANGE, queue_name,
                    self.ROUTING_KEY)
        cb = functools.partial(self.on_bindok, userdata=queue_name)
        self._channel.queue_bind(
            queue_name,
            self.EXCHANGE,
            routing_key=self.ROUTING_KEY,
            callback=cb)

    def on_bindok(self, _unused_frame, userdata):
        """Invoked by pika when the Queue.Bind method has completed. At this
        point we will set the prefetch count for the channel.

        :param pika.frame.Method _unused_frame: The Queue.BindOk response frame
        :param str|unicode userdata: Extra user data (queue name)

        """
        LOGGER.info('Queue bound: %s', userdata)
        self.set_qos()

    def set_qos(self):
        """This method sets up the consumer prefetch to only be delivered
        one message at a time. The consumer must acknowledge this message
        before RabbitMQ will deliver another one. You should experiment
        with different prefetch values to achieve desired performance.

        """
        self._channel.basic_qos(
            prefetch_count=self._prefetch_count, callback=self.on_basic_qos_ok)

    def on_basic_qos_ok(self, _unused_frame):
        """Invoked by pika when the Basic.QoS method has completed. At this
        point we will start consuming messages by calling start_consuming
        which will invoke the needed RPC commands to start the process.

        :param pika.frame.Method _unused_frame: The Basic.QosOk response frame

        """
        LOGGER.info('QOS set to: %d', self._prefetch_count)
        self.start_consuming()

    def start_consuming(self):
        """This method sets up the consumer by first calling
        add_on_cancel_callback so that the object is notified if RabbitMQ
        cancels the consumer. It then issues the Basic.Consume RPC command
        which returns the consumer tag that is used to uniquely identify the
        consumer with RabbitMQ. We keep the value to use it when we want to
        cancel consuming. The on_message method is passed in as a callback pika
        will invoke when a message is fully received.

        """
        LOGGER.info('Issuing consumer related RPC commands')
        self.add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(
            self.QUEUE, self.on_message)
        self.was_consuming = True
        self._consuming = True

    def add_on_cancel_callback(self):
        """Add a callback that will be invoked if RabbitMQ cancels the consumer
        for some reason. If RabbitMQ does cancel the consumer,
        on_consumer_cancelled will be invoked by pika.

        """
        LOGGER.info('Adding consumer cancellation callback')
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def on_consumer_cancelled(self, method_frame):
        """Invoked by pika when RabbitMQ sends a Basic.Cancel for a consumer
        receiving messages.

        :param pika.frame.Method method_frame: The Basic.Cancel frame

        """
        LOGGER.info('Consumer was cancelled remotely, shutting down: %r',
                    method_frame)
        if self._channel:
            self._channel.close()

    def on_message(self, _unused_channel, basic_deliver, properties, body):
        """Invoked by pika when a message is delivered from RabbitMQ. The
        channel is passed for your convenience. The basic_deliver object that
        is passed in carries the exchange, routing key, delivery tag and
        a redelivered flag for the message. The properties passed in is an
        instance of BasicProperties with the message properties and the body
        is the message that was sent.

        :param pika.channel.Channel _unused_channel: The channel object
        :param pika.Spec.Basic.Deliver: basic_deliver method
        :param pika.Spec.BasicProperties: properties
        :param bytes body: The message body

        """
        LOGGER.info('Received message # %s from %s: %s',
                    basic_deliver.delivery_tag, properties.app_id, body)
        self.acknowledge_message(basic_deliver.delivery_tag)

    def acknowledge_message(self, delivery_tag):
        """Acknowledge the message delivery from RabbitMQ by sending a
        Basic.Ack RPC method for the delivery tag.

        :param int delivery_tag: The delivery tag from the Basic.Deliver frame

        """
        LOGGER.info('Acknowledging message %s', delivery_tag)
        self._channel.basic_ack(delivery_tag)

    def stop_consuming(self):
        """Tell RabbitMQ that you would like to stop consuming by sending the
        Basic.Cancel RPC command.

        """
        if self._channel:
            LOGGER.info('Sending a Basic.Cancel RPC command to RabbitMQ')
            cb = functools.partial(
                self.on_cancelok, userdata=self._consumer_tag)
            self._channel.basic_cancel(self._consumer_tag, cb)

    def on_cancelok(self, _unused_frame, userdata):
        """This method is invoked by pika when RabbitMQ acknowledges the
        cancellation of a consumer. At this point we will close the channel.
        This will invoke the on_channel_closed method once the channel has been
        closed, which will in-turn close the connection.

        :param pika.frame.Method _unused_frame: The Basic.CancelOk frame
        :param str|unicode userdata: Extra user data (consumer tag)

        """
        self._consuming = False
        LOGGER.info(
            'RabbitMQ acknowledged the cancellation of the consumer: %s',
            userdata)
        self.close_channel()

    def close_channel(self):
        """Call to close the channel with RabbitMQ cleanly by issuing the
        Channel.Close RPC command.

        """
        LOGGER.info('Closing the channel')
        self._channel.close()

    def run(self):
        """Run the example consumer by connecting to RabbitMQ and then
        starting the IOLoop to block and allow the SelectConnection to operate.

        """
        self._connection = self.connect()
        self._connection.ioloop.start()

    def stop(self):
        """Cleanly shutdown the connection to RabbitMQ by stopping the consumer
        with RabbitMQ. When RabbitMQ confirms the cancellation, on_cancelok
        will be invoked by pika, which will then closing the channel and
        connection. The IOLoop is started again because this method is invoked
        when CTRL-C is pressed raising a KeyboardInterrupt exception. This
        exception stops the IOLoop which needs to be running for pika to
        communicate with RabbitMQ. All of the commands issued prior to starting
        the IOLoop will be buffered but not processed.

        """
        if not self._closing:
            self._closing = True
            LOGGER.info('Stopping')
            if self._consuming:
                self.stop_consuming()
                self._connection.ioloop.start()
            else:
                self._connection.ioloop.stop()
            LOGGER.info('Stopped')
