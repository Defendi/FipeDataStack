import json
import requests
import os
import logging
import time
from .fipe_api_service import FipeAPI

# Configuração do logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    fipe_api = FipeAPI()
    logger.info("Processing SQS messages...")
    new_sqs_queue_url = os.getenv("SQS_URL")  # URL da nova fila SQS

    batch = []
    batch_item_failures = []

    for record in event["Records"]:
        message_id = record["messageId"]
        message = json.loads(record["body"])
        logger.info(f"Message received: {message} (Message ID: {message_id})")
        message = json.loads(record["body"])
        logger.info(f"Message received: {message}")

        # Ajuste das chaves
        reference_table_code = message["codigoTabelaReferencia"]
        vehicle_type = message["vehicle_type"]
        manufacturer_code = message["manufacturer_code"]
        model_code = message["model_code"]
        year_model = message.get("anoModelo", "Unknown")
        manufacturer_name = message.get("manufacturer", "Unknown")
        model_name = message.get("model", "Unknown")
        reference_month_name = message.get("mesReferenciaAno", "Desconhecido")

        retries = 2
        delay = 5
        while retries > 0:
            try:
                # Obtém os anos e tipos de combustível disponíveis
                years, available_fuel_types = fipe_api.get_years(
                    manufacturer_code, model_code, vehicle_type
                )

                for year in years:
                    year_model = year.get("yearModel", "Unknown")
                    year_name = year.get("Label", "Unknown")
                    logger.info(f"Processing year: {year_model}, Label: {year_name}")

                    for fuel_type in available_fuel_types:
                        fuel_type_code = fuel_type.split("-")[-1]

                        logger.info(
                            f"Attempting to get price for fuel type: {fuel_type_code} (Year: {year_model}, Model: {model_name})"
                        )

                        price = fipe_api.get_price(
                            manufacturer_code,
                            model_code,
                            year_model,
                            vehicle_type,
                            fuel_type_code,
                        )

                        if price:
                            complete_data = {
                                "manufacturer": manufacturer_name,
                                "manufacturer_code": manufacturer_code,
                                "model": model_name,
                                "model_code": model_code,
                                "model_year": year_name,
                                "model_year_code": year_model,
                                "fipe_value": price.get("Valor", ""),
                                "fipe_code": price.get("CodigoFipe", ""),
                                "fuel_type": fuel_type_code,  # Use o código de combustível processado
                                "vehicle_type": vehicle_type,
                                "mesReferenciaAno": reference_month_name,
                                "codigoTabelaReferencia": reference_table_code,
                            }
                            logger.info(
                                f"Data to be sent: {json.dumps(complete_data, indent=4, ensure_ascii=False)}"
                            )
                            batch.append(complete_data)
                break

            except requests.HTTPError as e:
                if e.response.status_code == 429:
                    logger.warning(
                        "[429] - Rate limit exceeded. Waiting for 5 seconds..."
                    )
                    time.sleep(delay)
                    retries -= 1
                    delay *= 2  # Exponential backoff
                else:
                    logger.error(f"HTTP Error: {e}")
                    break

            except Exception as e:
                logger.error(f"Error processing message: {e}")
                break

    if batch:
        try:
            logger.info(f"Preparing to send batch to SQS: {batch}")
            failures = fipe_api.send_sqs_messages(
                new_sqs_queue_url, batch
            )  # Captura as falhas
            batch_item_failures.extend(failures)  # Adiciona as falhas à lista de falhas
            logger.info(f"Batch sent successfully.")
        except Exception as e:
            logger.error(f"Error sending batch to SQS: {e}")

    return {
        "statusCode": 200,
        "body": json.dumps("Processing completed successfully"),
        "batchItemFailures": batch_item_failures,
    }
