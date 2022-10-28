#######################################################################################
# LAMBDA FRIDAY API
# DESCRIPCION : Esta lambda entrega la lista de los pedidos del portal vendedor
# AUTOR : @koandina.com
# CAMBIOS :
# VER. FECHA AUTOR COMENTARIOS
# -------------------------------------------------------------------------------------
# 1.0 2021/07/07  Version Inicial
# -------------------------------------------------------------------------------------
#######################################################################################

import json
import pickle
import boto3
from io import BytesIO
import numpy as np
import logging

# Logger instance setting
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):

    # lectura del archivo almacenado en el buckets
    try:
        s3 = boto3.resource('s3')
        #main = pickle.loads(s3.Bucket("figuresonboarding").Object("cliente_pedidos.pickle").get()['Body'].read()).get(event["cliente"],{})
        main = pickle.loads(s3.Bucket("portalvendedor").Object(
            "pickles/cliente_pedidos.pickle").get()['Body'].read()).get(event["cliente"], {})
        logger.info('{0}..Recuperando S3 e informaacion Main' +
                    str(s3), (main))
# Manejo de Excepcion capturar el error retornado desde la Api del Cliente
    except Exception as e:
        r = {"statusCode": 400,
             "Error": "Datos Recibidos No Validos",
             "body": str(e)
             }
        # Manejo de Excepcion de lectura desde bucket de s3
        logger.error("{}... {}".format("Error: ", str(e)))

# Respuesta Correcta maneejo de cors
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type: application/json; charset=UTF-8',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,GET'
        },
        'body': main
    }
