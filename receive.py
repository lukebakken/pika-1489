import functools
import threading
import pika

def initiate_consumer():
    consumer_thread = threading.Thread(target=consumer, name="Consumer")
    consumer_thread.daemon = True
    consumer_thread.start()


def consumer():
    def ack_message(ch, delivery_tag):
        if ch.is_open:
            ch.basic_ack(delivery_tag)
        else:
            # Channel is already closed, so we can't ACK this message;
            # log and/or do something that makes sense for your app in this case.
            pass

    def do_work(conn, ch, delivery_tag, body):
        thread_id = threading.get_ident()
        print("Thread id: %s Delivery tag: %s ", thread_id, delivery_tag)
        json_body = json.loads(body.decode("utf-8"))
        job_id = json_body.get("id")
        data = json_body.get("data")
        start_data_upload(job_id, data)
        cb = functools.partial(ack_message, ch, delivery_tag)
        conn.add_callback_threadsafe(cb)

    def on_message(ch, method_frame, _header_frame, body, args):
        (conn, thrds) = args
        delivery_tag = method_frame.delivery_tag
        t = threading.Thread(target=do_work, args=(conn, ch, delivery_tag, body))
        t.start()
        thrds.append(t)

    try:
        credentials = pika.PlainCredentials(
            constants.rabbit_config["int"]["username"],
            constants.rabbit_config["int"]["password"],
        )
        context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
        context.verify_mode = ssl.CERT_REQUIRED
        context.load_verify_locations(cafile=constants.rabbit_config["int"]["certpath"])
        parameters = pika.ConnectionParameters(
            constants.rabbit_config["int"]["endpoint"],
            5671,
            "/",
            credentials,
            ssl_options=pika.SSLOptions(context),
        )
        connection = pika.BlockingConnection(parameters=parameters)
        channel = connection.channel()
        channel.exchange_declare(
            exchange="test_exchange",
            exchange_type=ExchangeType.direct,
            passive=False,
            durable=True,
            auto_delete=False,
        )
        channel.queue_declare(queue="hello", auto_delete=False)
        channel.queue_bind(queue="hello", exchange="test_exchange", routing_key="hello")
        channel.basic_qos(prefetch_count=1)

        threads = []
        on_message_callback = functools.partial(on_message, args=(connection, threads))
        channel.basic_consume(on_message_callback=on_message_callback, queue="hello")

        try:
            print(" [*] Waiting for messages. To exit press CTRL+C")
            channel.start_consuming()
        except KeyboardInterrupt as e:
            db_logger.debug(f"RabbitMQ Stopped due to KeyboardInterrupt:,{e}")
            print("KeyboardInterrupt rabbit exception")
            channel.stop_consuming()
        except Exception as e:
            print("inner rabbit exception")
            db_logger.debug(f"RabbitMQ Stopped:,{e}")
            print(traceback.print_exc())
            timer = threading.Timer(20, initiate_consumer)
            timer.start()

        # Wait for all to complete

        for thread in threads:
            thread.join()
        connection.close()
    except Exception as e:
        print("main rabbit exception")
        db_logger.debug(f"RabbitMQ Stopped:,{e}")
        print(traceback.print_exc())
