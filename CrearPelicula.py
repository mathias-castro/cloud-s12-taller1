import boto3
import uuid
import os
import json

def lambda_handler(event, context):
    try:
        # Entrada (json)
        log_info = {
            "tipo": "INFO",
            "log_datos": {
                "mensaje": "Iniciando creación de película",
                "evento": event
            }
        }
        print(json.dumps(log_info))
        
        tenant_id = event['body']['tenant_id']
        pelicula_datos = event['body']['pelicula_datos']
        nombre_tabla = os.environ["TABLE_NAME"]
        
        # Proceso
        uuidv4 = str(uuid.uuid4())
        pelicula = {
            'tenant_id': tenant_id,
            'uuid': uuidv4,
            'pelicula_datos': pelicula_datos
        }
        
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(nombre_tabla)
        response = table.put_item(Item=pelicula)
        
        # Salida (json)
        log_success = {
            "tipo": "INFO",
            "log_datos": {
                "mensaje": "Película creada exitosamente",
                "pelicula": pelicula,
                "dynamodb_response": str(response)
            }
        }
        print(json.dumps(log_success))
        
        return {
            'statusCode': 200,
            'pelicula': pelicula,
            'response': response
        }
    
    except KeyError as e:
        log_error = {
            "tipo": "ERROR",
            "log_datos": {
                "mensaje": "Error: Falta un campo requerido",
                "error": str(e),
                "tipo_error": "KeyError",
                "evento": event
            }
        }
        print(json.dumps(log_error))
        
        return {
            'statusCode': 400,
            'error': f'Campo requerido faltante: {str(e)}'
        }
    
    except Exception as e:
        log_error = {
            "tipo": "ERROR",
            "log_datos": {
                "mensaje": "Error inesperado al crear película",
                "error": str(e),
                "tipo_error": type(e).__name__,
                "evento": event
            }
        }
        print(json.dumps(log_error))
        
        return {
            'statusCode': 500,
            'error': f'Error interno: {str(e)}'
        }
