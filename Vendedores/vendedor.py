#######################################################################################
# LAMBDA FRIDAY API
# DESCRIPCION : Esta lambda entrega los datos del vendedor
# AUTOR : @koandina.com
# CAMBIOS :
# VER. FECHA AUTOR COMENTARIOS
# -------------------------------------------------------------------------------------
# 1.0 2021/07/07  Version Inicial
# -------------------------------------------------------------------------------------
#######################################################################################
# Importaciones de librerias

import json
import pickle
import boto3
from io import BytesIO
import numpy as np
import logging


# Logger instance setting
logger = logging.getLogger()
logger.setLevel(logging.INFO)


# Function Principal
def lambda_handler(event, context):

    # lectura del archivo almacenado en el buckets
    if event["verificador"] == 'icalderon@koandina.com':
        event["verificador"] = 'ccarrascor@koandina.com'
    if event['verificador'] == 'nvalderrama@koandina.com':
        event['verificador'] = 'eperez@koandina.com'
    if event['verificador'] == 'csilvat@koandina.com':
        event['verificador'] = 'furriola@koandina.com'
    if event['verificador'] == 'tro-azapata@koandina.com':
        event['verificador'] = 'FJRAMOS@KOANDINA.COM'

    event['verificador'] = event['verificador'].upper()
    # try:
    s3 = boto3.resource('s3')
    logger.info('conexión a S3 exisota')
    logger.info(event['verificador'])
    main = pickle.loads(s3.Bucket("portalvendedor").Object(
        "pickles/ruta_vendedor.pickle").get()['Body'].read()).get(event["verificador"], {})


# Manejo de Excepcion capturar el error retornado desde la Api del Cliente
    # except Exception as e:
    #    r={"statusCode": 400,
    #      "Error": "Datos Recibidos No Validos",
    #      "body": str(e)
    #      }
    #      #Manejo de Excepcion de lectura desde bucket de s3
    #    logger.error("{}... {}".format("Error: " ,str(e)) )

    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type: application/json; charset=UTF-8',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,GET'
        },
        'body': main
    }
