from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
import uuid
import boto3
def lambda_handler(event, context):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        response = page.goto("https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados")

        if response.status != 200:
            return {
                'statusCode': response.status,
                'body': 'Error al acceder a la página web'
            }

        # Esperar a que se cargue la tabla
        page.wait_for_selector("table")

        # Obtener el HTML
        html = page.content()
        browser.close()

        # Parsear con BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")
        table = soup.find("table")

        if not table:
            return {
                'statusCode': 404,
                'body': 'No se encontró la tabla en la página web'
            }

        headers = [header.text for header in table.find_all('th')[:-1]]

        rows = []
        for row in table.find_all('tr')[1:]:  # Omitir el encabezado
            cells = row.find_all('td')[:-1]  # Extraer todas las celdas de la fila
            rows.append({headers[i]: cell.text.strip() for i, cell in enumerate(cells)})

        # Guardar los datos en DynamoDB
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('TablaWebScrappingSismos')

        # Eliminar todos los elementos de la tabla antes de agregar los nuevos
        scan = table.scan()
        with table.batch_writer() as batch:
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
            table.put_item(Item=row)
            i = i + 1

        # Retornar el resultado como JSON
        return {
            'statusCode': 200,
            'body': rows
        }
