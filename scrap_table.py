import os
import requests
from bs4 import BeautifulSoup
import uuid
import boto3

def lambda_handler(event, context):
    # Obtén la clave de API de las variables de entorno
    api_key = os.environ.get("API_KEY")

    if not api_key:
        return {
            'statusCode': 500,
            'body': "La clave de API no está configurada en las variables de entorno"
        }

    # URL de la página que deseas scrapear
    url = "https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados"

    # Construir la URL de ScraperAPI
    scraperapi_url = f"https://api.scraperapi.com/?api_key={api_key}&url={url}&render=true"

    try:
        # Hacer la solicitud a ScraperAPI
        response = requests.get(scraperapi_url)

        if response.status_code != 200:
            return {
                'statusCode': response.status_code,
                'body': f"Error al obtener la página: {response.text}"
            }
        html = response.text  # HTML renderizado
        print(html)
    except Exception as e:
        return {
            'statusCode': 500,
            'body': f"Error al acceder a la página web: {str(e)}"
        }

    # Parsear el contenido HTML con BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")

    # Encontrar la tabla
    table = soup.find("table")
    if not table:
        return {
            'statusCode': 404,
            'body': 'No se encontró la tabla en la página web'
        }

    # Extraer encabezados
    headers = [header.text.strip() for header in table.find_all('th')[:-1]]

    # Extraer filas de la tabla
    rows = []
    for row in table.find_all('tr')[1:]:  # Omitir el encabezado
        cells = row.find_all('td')[:-1]  # Extraer todas las celdas de la fila
        rows.append({headers[i]: cell.text.strip() for i, cell in enumerate(cells)})
   
    # Guardar los datos en DynamoDB
    dynamodb = boto3.resource('dynamodb')
    dynamodb_table = dynamodb.Table('TablaWebScrappingSismos')

    # Eliminar todos los elementos de la tabla antes de agregar los nuevos
    scan = dynamodb_table.scan()
    with dynamodb_table.batch_writer() as batch:
        for each in scan['Items']:
            batch.delete_item(
                Key={
                    'id': each['id']
                }
            )

    # Insertar los nuevos datos
    i = 1
    for row in rows:
        row['#'] = i
        row['id'] = str(uuid.uuid4())  # Generar un ID único para cada entrada
        dynamodb_table.put_item(Item=row)
        i += 1

    # Retornar el resultado como JSON
    return {
        'statusCode': 200,
        'body': rows
    }
