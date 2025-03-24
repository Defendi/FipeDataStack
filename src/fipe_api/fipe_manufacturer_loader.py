import os
import time
import json
import argparse
from fipe_api_service import FipeAPI

def process_vehicle_types(is_local=False, local_output_file=None):
    """
    Função principal que processa os tipos de veículos
    
    Args:
        is_local (bool): Indica se está rodando localmente
        local_output_file (str): Caminho para arquivo de saída local (quando is_local=True)
    """
    fipe_api = FipeAPI()
    queue_url = os.getenv('SQS_URL', 'https://sqs.dummy-url-for-local-testing.com')
    vehicle_types = [3, 1, 2]  # 1: Car, 2: Motorcycle, 3: Truck
    delay = 1.0  # Delay aumentado para 1 segundo

    # Para armazenar mensagens localmente em vez de enviar para SQS
    local_messages = []

    for vehicle_type in vehicle_types:
        try:
            print(f"Starting process for vehicle type {vehicle_type}...")
            brands = fipe_api.get_brands(vehicle_type)
            
            if not brands:
                print(f"No brands found for vehicle type {vehicle_type}.")
                continue

            print(f"Found {len(brands)} brands for vehicle type {vehicle_type}.")

            for brand in brands:
                brand_code = brand.get('Value')
                brand_name = brand.get('Label')
                
                if not (brand_code and brand_name):
                    print(f"Missing 'Value' or 'Label' in brand: {brand}")
                    continue

                print(f"Processing brand '{brand_name}' (Code: {brand_code}) for vehicle type {vehicle_type}.")

                message = {
                    "codigoTabelaReferencia": fipe_api.reference_table_code,
                    "mesReferenciaAno": fipe_api.reference_month_name,
                    "codigoMarca": brand_code,
                    "nomeMarca": brand_name,
                    "codigoTipoVeiculo": vehicle_type
                }
                
                if is_local:
                    # Salvar mensagem localmente em vez de enviar para SQS
                    local_messages.append(message)
                    print(f"Locally saved message for brand '{brand_name}'")
                else:
                    # Enviar para SQS quando estiver no Lambda
                    fipe_api.send_message_sqs(queue_url, message)
                
                time.sleep(delay)

        except Exception as e: 
            print(f"Error processing vehicle type {vehicle_type}: {e}")

    # Se estiver rodando localmente e tiver mensagens, salva em arquivo
    if is_local and local_messages and local_output_file:
        try:
            with open(local_output_file, 'w', encoding='utf-8') as f:
                json.dump(local_messages, f, ensure_ascii=False, indent=2)
            print(f"Successfully saved {len(local_messages)} messages to {local_output_file}")
        except Exception as e:
            print(f"Error saving local messages to file: {e}")

    print("Processing completed for all vehicle types.")
    return {
        'statusCode': 200,
        'body': 'Processing completed successfully!',
        'message_count': len(local_messages) if is_local else None
    }

def lambda_handler(event, context):
    """
    Handler para AWS Lambda
    """
    return process_vehicle_types(is_local=False)

if __name__ == "__main__":
    """
    Ponto de entrada para execução local
    """
    parser = argparse.ArgumentParser(description='Process FIPE manufacturers data')
    parser.add_argument('--output', '-o', default='fipe_messages.json',
                      help='Output file for local testing (default: fipe_messages.json)')
    args = parser.parse_args()
    
    print("Running in local mode...")
    result = process_vehicle_types(is_local=True, local_output_file=args.output)
    print(f"Completed with status code: {result['statusCode']}")
    print(f"Processed {result['message_count']} messages")