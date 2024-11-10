from requests_html import HTMLSession
from bs4 import BeautifulSoup
import uuid
import boto3

def lambda_handler(event, context):
    session = HTMLSession()
    try:
        # Hacer la solicitud HTTP y renderizar la página con JavaScript
        response = session.get("https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados")
        response.html.render()
        # response.html.render()  # Renderizar el contenido dinámico (JavaScript)
        html = response.html.html  # Obtener el HTML renderizado
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

    print(rows)

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


