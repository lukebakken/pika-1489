def upload_initial_data(cls):
    logging.info("Loading data..")
    route = ware_house_obj = None
    file_path = os.path.join(settings.ZIP_DOWNLOAD_DIR, "TRANSACTIONAL_DATA.csv")
    file = open(file_path, "r", encoding="utf-8-sig")
    csv_object = csv.reader(file, dialect="excel", delimiter=",", quotechar='"')

    fieldnames = (
        "Wh",
        "Route",
        "Date",
        "Time",
        "Time_zone",
        "Dealer",
        "EWM_SPICS_delivery",
        "Item_EWM_SPICS",
        "Product",
        "Product_Description",
        "Quantity",
        "HU",
        "Gitter_Box",
        "NFN",
    )
    try:
        reader = csv.DictReader(
            file, fieldnames=fieldnames, dialect="excel", delimiter=","
        )
        csv_rows = []
        for index, row in enumerate(reader):
            if index == 0:
                try:
                    if row["Wh"] is not None:
                        warehouse_id = row["Wh"]
                        if not isinstance(warehouse_id, str):
                            warehouse_id = str(warehouse_id)
                        ware_house_obj = WareHouse.objects.get(
                            warehouse_id=warehouse_id
                        )
                        print(
                            "Warehouse available: {}".format(
                                ware_house_obj.warehouse_id
                            )
                        )
                        db_logger.debug(
                            f"Warehouse available::,{ware_house_obj.warehouse_id}"
                        )
                    else:
                        print("Warehouse not available: {}".format(warehouse_id))
                        db_logger.debug(f"Warehouse not available::,{warehouse_id}")
                        return
                except Exception as e:
                    print("Warehouse not available ex: {}".format(warehouse_id))
                    db_logger.debug(f"Warehouse not available ex:,{warehouse_id}")
                    return

            csv_rows.extend(
                [{fieldnames[i]: row[fieldnames[i]] for i in range(len(fieldnames))}]
            )
        job_id = uuid.uuid4()
        # connection string and initialization
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
        channel.queue_declare(queue="hello")
        channel.basic_publish(
            exchange="test_exchange",
            routing_key="hello",
            body=json.dumps({"data": csv_rows, "id": str(job_id)}),
        )
        connection.close()
        job_obj = TransactionalQueueJob.objects.create(
            job_id=job_id, job_data=csv_rows, status=1
        )
    except Exception as e:
        print(traceback.print_exc())
        db_logger.exception(e)
