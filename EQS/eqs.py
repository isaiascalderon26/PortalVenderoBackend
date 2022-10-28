#######################################################################################
# LAMBDA FRIDAY API
# DESCRIPCION : Esta lambda entrega informaacion para eel portal vendedor
# AUTOR : @koandina.com
# CAMBIOS :
# VER. FECHA AUTOR COMENTARIOS
# -------------------------------------------------------------------------------------
# 1.0 2021/07/07  Version Inicial
# -------------------------------------------------------------------------------------
#######################################################################################

#Importaacion de librerias

import json
import pickle
import boto3
from io import BytesIO
import numpy as np

#Funcion Principal 
def lambda_handler(event, context):

#lectura del archivo almacenado en el buckets
    try:
        s3 = boto3.resource('s3')
        main = pickle.loads(s3.Bucket("figuresonboarding").Object("cliente_eqs.pickle").get()['Body'].read())[event["cliente"]]
        #main = pickle.loads(s3.Bucket("portalvendedor").Object("cliente_eqs.pickle").get()['Body'].read())[event["cliente"]]

#Manejo de Excepcion capturar el error retornado desde la Api del Cliente
    except Exception as e:
        r={"statusCode": 400,
          "Error": "Datos Recibidos No Validos",
          "body": str(e) 
          }
          #Manejo de Excepcion de lectura desde bucket de s3  
        logger.error("{}... {}".format("Error: " ,str(e)) )

#Respuesta Correcta maneejo de cors      
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,GET'
        },
        'body': main
    }
