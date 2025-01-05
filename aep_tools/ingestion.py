import requests
import json
import time

class AEPIngestion:
    def __init__(self, headers):
        """
        Inicializa a classe com os headers necessários para autenticação.
        :param headers: Headers incluindo Authorization, API Key, etc.
        """
        self.headers = headers

    def create_source_connection(self, payload):
        """
        Cria uma conexão de origem no AEP.
        :param payload: Dicionário com os dados da conexão de origem.
        :return: Resposta da API como JSON.
        """
        url = 'https://platform.adobe.io/data/foundation/flowservice/sourceConnections'
        response = requests.post(url, headers=self.headers, json=payload)
        return response.json()

    def create_target_connection(self, payload):
        """
        Cria uma conexão de destino no AEP.
        :param payload: Dicionário com os dados da conexão de destino.
        :return: Resposta da API como JSON.
        """
        url = 'https://platform.adobe.io/data/foundation/flowservice/targetConnections'
        response = requests.post(url, headers=self.headers, json=payload)
        return response.json()

    def create_mapping(self, data):
        """
        Cria um mapeamento para dados no AEP.
        :param data: Dicionário com os dados do mapeamento.
        :return: Resposta da API como JSON.
        """
        url = 'https://platform.adobe.io/data/foundation/conversion/mappingSets'
        response = requests.post(url, headers=self.headers, data=json.dumps(data))
        return response.json()

    def create_dataflow(self, payload):
        """
        Cria um dataflow no AEP.
        :param payload: Dicionário com os dados do dataflow.
        :return: Resposta da API como JSON.
        """
        url = "https://platform.adobe.io/data/foundation/flowservice/flows"
        response = requests.post(url, headers=self.headers, json=payload)
        return response.json()

    @staticmethod
    def get_future_timestamp(minutes=2):
        """
        Calcula um timestamp futuro com base no tempo atual.
        :param minutes: Minutos a adicionar ao timestamp atual.
        :return: Timestamp futuro.
        """
        current_time = int(time.time())
        return current_time + (minutes * 60)
