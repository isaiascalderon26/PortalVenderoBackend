#######################################################################################
# LAMBDA FRIDAY API
# DESCRIPCION : Esta lambda entrega la lista de clientes o un la ainformacion de un cliente
# AUTOR : @koandina.com
# CAMBIOS :
# VER. FECHA AUTOR COMENTARIOS
# -------------------------------------------------------------------------------------
# 1.0 2021/07/07  Version Inicial
# -------------------------------------------------------------------------------------
#######################################################################################

# Importacion de librerias

#import awswrangler as wr
import json
import logging
import os
from urllib.parse import urlparse
import urllib.request
import urllib.parse
from datetime import datetime
import pandas as pd
try:
    from urllib.parse import urlparse

except ImportError:
    from urlparse import urlparse

#import requests
import pickle
import boto3
from io import BytesIO
import numpy as np
#import base64
#from botocore.exceptions import ClientError


# Logger instance setting
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Funcion Principal


def lambda_handler(event, context):

    weektoday = datetime.today().weekday()
    days_txt = {0: 'L', 1: 'M', 2: 'W', 3: 'J', 4: 'V', 5: 'S', 6: 'D'}
    weektoday = days_txt.get(weektoday, '')
    # Esto mientras no esté la data de otros días
    weektoday = 'V'
    logger.info(weektoday)
# lectura del archivo almacenado en el buckets
    try:
        s3 = boto3.resource('s3')
        if event["ruta"] == 'Isaias Abrahans Calderon Perez':
            event["ruta"] = 'CF61'

        grupo = pickle.loads(s3.Bucket("portalvendedor").Object(
            "pickles/pivote_grupo_vendedor/ruta_grupo.pickle").get()['Body'].read()).get(event["ruta"], '')
        main = pickle.loads(s3.Bucket("portalvendedor").Object(
            "pickles/"+grupo+"/"+grupo+"ruta_clientes.pickle").get()['Body'].read()).get(event["ruta"], {})
        print(main[0:1])
        print('---------------------------')
        #main = pickle.loads(s3.Bucket("portalvendedor").Object("pickles/ruta_clientes.pickle").get()['Body'].read()).get(event["ruta"],{})
        # print(main[0:1])
        if event.get('cliente', '')[0:2] == '05':
            main = [x for x in main if x['kunnr'] == event['cliente']]
        if event.get('cliente', '')[0:2] != '05' and event.get('cliente', '') != '':
            main = [x for x in main if x['txtmd'] == event['cliente']]


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
