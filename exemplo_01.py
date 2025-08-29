import requests
from tinydb import TinyDB
from datetime import datetime

def extrair():
    url = "https://api.coinbase.com/v2/prices/spot"
    response = requests.get(url)
    return response.json()

def transformar(dados):
    
  
    valor = dados['data']['amount']
    criptomoeda = dados['data']['base']
    moeda = dados['data']['currency']
    
    dados_tratados = { 
      "valor": valor, 
      "criptomoeda": criptomoeda,
      "moeda": moeda, 
      "timesTamp": datetime.now().strftime("%d/%m/%Y, %H:%M:%S")
      }
    return dados_tratados

def load(dados_tratados):
    db = TinyDB('db.json')
    db.insert(dados_tratados)
    print("Dados salvos com sucesso!")


if __name__ == "__main__":
    dados_extraidos = extrair()
    tratados = transformar(dados_extraidos)
    
    load(tratados)
    
    
    
