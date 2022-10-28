import json
import pandas as pd
import pickle
import boto3

s3_client = boto3.client('s3')

#################################
#### Funciones preeliminares ####
#################################

# Lectura desde S3


def read_tbl(file):
    bucket_name = 'portalvendedor'
    s3_file_name = file
    resp = s3_client.get_object(Bucket=bucket_name, Key=s3_file_name)
    return pd.read_csv(BytesIO(resp['Body'].read()))

# Función correctora usuario vendedor


def ver_vendedores(x):
    if x.DSEML == "@KOANDINA.COM":
        return x.CDUSU
    else:
        return x.DSEML

############################
#### Funcioón principal ####
############################


def lambda_handler(event, context):
    maestra_vendedores = read_tbl('xtract_vendedores/vendedores.csv')

    maestra_vendedores.fillna('', inplace=True)
    maestra_vendedores.CDUSU = maestra_vendedores.CDUSU.apply(str)
    maestra_vendedores['verificador'] = maestra_vendedores[[
        'CDUSU', 'DSEML']].apply(ver_vendedores, axis=1)
    maestra_vendedores.DSNOME = maestra_vendedores.DSNOME.apply(
        lambda x: x.replace('Ã\x89', 'É'))

    cols = ['CDUSU', 'DSNOME', 'DSLOGN', 'CDEQP']
    cols_txt = {'CDUSU': 'cod_user', 'DSNOME': 'nombre_user',
                'DSLOGN': 'rut_user', 'CDEQP': 'ruta'}
    ruta_vendedor = {}
    for i in range(len(maestra_vendedores)):
        ruta_vendedor[maestra_vendedores.verificador[i]] = {
            cols_txt[col]: maestra_vendedores[col][i] for col in cols}

    obj_main = pickle.dumps(ruta_vendedor)
    s3_client.put_object(Bucket='portalvendedor',
                         Key='pickles/ruta_vendedor.pickle', Body=obj_main)

    return {
        'statusCode': 200,
        'body': json.dumps('Job done!')
    }
