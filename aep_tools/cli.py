import click
import requests
import json
import sys
import time
from aep_tools.ingestion import AEPIngestion


def generate_access_token(client_id, client_secret, scopes):
    """
    Gera o Access Token usando as credenciais fornecidas.
    """
    response = requests.post('https://ims-na1.adobelogin.com/ims/token/v3', data={
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": ','.join(scopes)
    })

    if response.ok:
        return response.json()['access_token']
    else:
        click.echo("Falha na geração do Access Token.")
        click.echo(response.text)
        sys.exit(1)


@click.group()
def cli():
    """Ferramenta CLI para facilitar a integração com Adobe Experience Platform."""
    pass


@cli.command()
@click.option('--client-id', required=True, help="Client ID (API Key).")
@click.option('--client-secret', required=True, help="Client Secret.")
@click.option('--ims-org-id', required=True, help="IMS Org ID.")
@click.option('--scopes', required=True, help="Lista de escopos, separados por vírgula.")
@click.option('--sandbox', required=True, help="Nome do sandbox (ex: 'prod' ou 'dev').")
@click.option('--name', required=True, help="Nome da conexão de origem.")
@click.option('--table-name', required=True, help="Nome da tabela no BigQuery.")
@click.option('--base-connection-id', required=True, help="ID da conexão base.")
@click.option('--columns', required=True, help="Colunas no formato JSON.")
def create_source(client_id, client_secret, ims_org_id, scopes, sandbox, name, table_name, base_connection_id, columns):
    """
    Cria uma conexão de origem no Adobe Experience Platform.
    """
    scopes_list = scopes.split(',')

    # Gerar Token de Acesso
    access_token = generate_access_token(client_id, client_secret, scopes_list)

    headers = {
        "Authorization": f"Bearer {access_token}",
        "x-api-key": client_id,
        "x-gw-ims-org-id": ims_org_id,
        "Content-Type": "application/json",
        "x-sandbox-name": sandbox
    }
    aep = AEPIngestion(headers=headers)

    payload = {
        "name": name,
        "baseConnectionId": base_connection_id,
        "description": "BigQuery Source Connection",
        "data": {"format": "tabular"},
        "params": {
            "tableName": table_name,
            "columns": json.loads(columns)
        },
        "connectionSpec": {
            "id": "3c9b37f8-13a6-43d8-bad3-b863b941fedd",
            "version": "1.0"
        }
    }

    response = aep.create_source_connection(payload)
    click.echo(f"Resposta: {response}")


@cli.command()
@click.option('--client-id', required=True, help="Client ID (API Key).")
@click.option('--client-secret', required=True, help="Client Secret.")
@click.option('--ims-org-id', required=True, help="IMS Org ID.")
@click.option('--scopes', required=True, help="Lista de escopos, separados por vírgula.")
@click.option('--sandbox', required=True, help="Nome do sandbox (ex: 'prod' ou 'dev').")
@click.option('--name', required=True, help="Nome da conexão de destino.")
@click.option('--dataset-id', required=True, help="ID do dataset de destino.")
@click.option('--schema-id', required=True, help="ID do schema do dataset de destino.")
def create_target(client_id, client_secret, ims_org_id, scopes, sandbox, name, dataset_id, schema_id):
    """
    Cria uma conexão de destino no Adobe Experience Platform.
    """
    scopes_list = scopes.split(',')

    # Gerar Token de Acesso
    access_token = generate_access_token(client_id, client_secret, scopes_list)

    headers = {
        "Authorization": f"Bearer {access_token}",
        "x-api-key": client_id,
        "x-gw-ims-org-id": ims_org_id,
        "Content-Type": "application/json",
        "x-sandbox-name": sandbox
    }

    payload = {
        "name": name,
        "description": "Target Connection",
        "data": {
            "schema": {
                "id": schema_id,
                "version": "application/vnd.adobe.xed-full+json;version=1"
            }
        },
        "params": {
            "dataSetId": dataset_id
        },
        "connectionSpec": {
            "id": "c604ff05-7f1a-43c0-8e18-33bf874cb11c",
            "version": "1.0"
        }
    }

    response = requests.post(
        'https://platform.adobe.io/data/foundation/flowservice/targetConnections',
        headers=headers,
        json=payload
    )

    if response.ok:
        click.echo(f"Conexão de destino criada com sucesso! ID: {response.json().get('id')}")
    else:
        click.echo("Erro ao criar conexão de destino:")
        click.echo(response.text)


@cli.command()
@click.option('--client-id', required=True, help="Client ID (API Key).")
@click.option('--client-secret', required=True, help="Client Secret.")
@click.option('--ims-org-id', required=True, help="IMS Org ID.")
@click.option('--scopes', required=True, help="Lista de escopos, separados por vírgula.")
@click.option('--sandbox', required=True, help="Nome do sandbox (ex: 'prod' ou 'dev').")
@click.option('--schema-id', required=True, help="ID do schema usado para o mapeamento.")
@click.option('--mappings', required=True, help="Lista de mapeamentos no formato JSON.")
def create_mapping(client_id, client_secret, ims_org_id, scopes, sandbox, schema_id, mappings):
    """
    Cria um mapeamento no Adobe Experience Platform.
    """
    scopes_list = scopes.split(',')

    # Gerar Token de Acesso
    access_token = generate_access_token(client_id, client_secret, scopes_list)

    headers = {
        "Authorization": f"Bearer {access_token}",
        "x-api-key": client_id,
        "x-gw-ims-org-id": ims_org_id,
        "Content-Type": "application/json",
        "x-sandbox-name": sandbox
    }

    payload = {
        "version": 0,
        "xdmSchema": schema_id,
        "xdmVersion": "1.0",
        "mappings": json.loads(mappings)
    }

    response = requests.post(
        'https://platform.adobe.io/data/foundation/conversion/mappingSets',
        headers=headers,
        json=payload
    )

    if response.ok:
        click.echo(f"Mapeamento criado com sucesso! ID: {response.json().get('id')}")
    else:
        click.echo("Erro ao criar mapeamento:")
        click.echo(response.text)


@cli.command()
@click.option('--client-id', required=True, help="Client ID (API Key).")
@click.option('--client-secret', required=True, help="Client Secret.")
@click.option('--ims-org-id', required=True, help="IMS Org ID.")
@click.option('--scopes', required=True, help="Lista de escopos, separados por vírgula.")
@click.option('--sandbox', required=True, help="Nome do sandbox (ex: 'prod' ou 'dev').")
@click.option('--name', required=True, help="Nome do fluxo de dados.")
@click.option('--description', required=True, help="Descrição do fluxo de dados.")
@click.option('--source-id', required=True, help="ID da conexão de origem.")
@click.option('--target-id', required=True, help="ID da conexão de destino.")
@click.option('--mapping-id', required=True, help="ID do mapeamento.")
@click.option('--start-time-minutes', default=60, help="Tempo em minutos para iniciar o fluxo. Padrão: 60 minutos (1 hora no futuro).")
@click.option('--frequency', required=True, help="Frequência do fluxo (ex: Day, Hour, Minute).")
@click.option('--interval', required=True, help="Intervalo da frequência (ex: 1 para diário).")
@click.option('--delta-column', required=True, help="Nome da coluna delta.")
@click.option('--date-format', required=True, help="Formato da data da coluna delta.")
@click.option('--timezone', required=True, help="Fuso horário da coluna delta.")
@click.option('--backfill', required=True, help="Se o fluxo inicial deve carregar dados antigos (true/false).")
def create_dataflow(client_id, client_secret, ims_org_id, scopes, sandbox, name, description, source_id, target_id, mapping_id, start_time_minutes, frequency, interval, delta_column, date_format, timezone, backfill):
    """
    Cria um fluxo de dados no Adobe Experience Platform.
    """
    scopes_list = scopes.split(',')

    # Gerar Token de Acesso
    access_token = generate_access_token(client_id, client_secret, scopes_list)

    headers = {
        "Authorization": f"Bearer {access_token}",
        "x-api-key": client_id,
        "x-gw-ims-org-id": ims_org_id,
        "Content-Type": "application/json",
        "x-sandbox-name": sandbox
    }

    # Garantir que o startTime esteja em pelo menos uma hora no futuro
    current_time = int(time.time())
    future_time = current_time + (start_time_minutes * 60)  # Adiciona tempo em minutos ao tempo atual

    payload = {
        "name": name,
        "description": description,
        "flowSpec": {
            "id": "14518937-270c-4525-bdec-c2ba7cce3860",
            "version": "1.0"
        },
        "sourceConnectionIds": [source_id],
        "targetConnectionIds": [target_id],
        "transformations": [
            {
                "name": "Copy",
                "params": {
                    "deltaColumn": {
                        "name": delta_column,
                        "dateFormat": date_format,
                        "timezone": timezone
                    }
                }
            },
            {
                "name": "Mapping",
                "params": {
                    "mappingId": mapping_id,
                    "mappingVersion": 0
                }
            }
        ],
        "scheduleParams": {
            "startTime": future_time,
            "frequency": frequency,
            "interval": interval,
            "backfill": backfill
        }
    }

    response = requests.post(
        'https://platform.adobe.io/data/foundation/flowservice/flows',
        headers=headers,
        json=payload
    )

    if response.ok:
        click.echo(f"Dataflow criado com sucesso! ID: {response.json().get('id')}")
    else:
        click.echo("Erro ao criar dataflow:")
        click.echo(response.text)


@cli.command()
@click.option('--client-id', required=True, help="Client ID (API Key).")
@click.option('--client-secret', required=True, help="Client Secret.")
@click.option('--ims-org-id', required=True, help="IMS Org ID.")
@click.option('--scopes', required=True, help="Lista de escopos, separados por vírgula.")
@click.option('--sandbox', required=True, help="Nome do sandbox (ex: 'prod' ou 'dev').")
@click.option('--dataset-ids', required=True, help="Lista de DataSet IDs desejados, separados por vírgulas.")
def track_flows(client_id, client_secret, ims_org_id, scopes, sandbox, dataset_ids):
    """
    Rastreia os DataFlows e exibe informações sobre Target Connections que correspondem aos DataSet IDs desejados.
    """
    scopes_list = scopes.split(',')
    access_token = generate_access_token(client_id, client_secret, scopes_list)

    headers = {
        "Authorization": f"Bearer {access_token}",
        "x-api-key": client_id,
        "x-gw-ims-org-id": ims_org_id,
        "Content-Type": "application/json",
        "x-sandbox-name": sandbox
    }

    # Converter os DataSet IDs desejados em um conjunto
    desired_dataset_ids = {id.strip() for id in dataset_ids.split(',')}
    click.echo(f"DataSet IDs fornecidos: {desired_dataset_ids}")

    url = "https://platform.adobe.io/data/foundation/flowservice/flows?"
    flow_response = requests.get(url, headers=headers)

    # Log da resposta da API /flows
    click.echo(f"Status da API /flows: {flow_response.status_code}")
    if flow_response.status_code == 200:
        flow_response_json = flow_response.json()
        flows = flow_response_json.get("items", [])
        click.echo(f"Flows retornados pela API: {len(flows)}")

        flow_targets = [
            {
                "name": flow["name"],
                "targetConnectionIds": flow["targetConnectionIds"]
            }
            for flow in flows if "targetConnectionIds" in flow
        ]

        target_connection_datasets = []

        for flow_target in flow_targets:
            for target_id in flow_target["targetConnectionIds"]:
                target_url = f"https://platform.adobe.io/data/foundation/flowservice/targetConnections/{target_id}"
                target_response = requests.get(target_url, headers=headers)

                if target_response.status_code == 200:
                    target_data = target_response.json()
                    click.echo(f"Dados retornados para Target ID {target_id}: {target_data}")

                    if (
                        "params" in target_data
                        and "dataSetId" in target_data["params"]
                        and "dataSetName" in target_data["params"]
                    ):
                        dataSetId = target_data["params"]["dataSetId"]
                        dataSetName = target_data["params"]["dataSetName"]
                        if dataSetId in desired_dataset_ids:
                            target_connection_datasets.append({
                                "flowName": flow_target["name"],
                                "targetConnectionId": target_id,
                                "dataSetId": dataSetId,
                                "dataSetName": dataSetName
                            })
                else:
                    click.echo(f"Falha ao buscar targetConnectionId {target_id}: {target_response.status_code}")
                    click.echo(target_response.json())

        if target_connection_datasets:
            click.echo("\nResultados Filtrados:")
            for detail in target_connection_datasets:
                click.echo(f"Flow Name: {detail['flowName']}")
                click.echo(f"Target Connection ID: {detail['targetConnectionId']}")
                click.echo(f"DataSet ID: {detail['dataSetId']}")
                click.echo(f"DataSet Name: {detail['dataSetName']}")
                click.echo()  # linha em branco para separar os resultados
        else:
            click.echo("Nenhum DataFlow corresponde aos DataSet IDs fornecidos.")
    else:
        click.echo(f"Falha ao buscar dados: {flow_response.status_code}")
        click.echo(flow_response.json())

if __name__ == "__main__":
    cli()
