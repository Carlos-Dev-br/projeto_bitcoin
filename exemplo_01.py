import requests
from tinydb import TinyDB
from sqlalchemy import create_engine, Column, String, Float, Integer, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime
from time import sleep

#Configurações do banco de dados
DATABASE_URL = "postgresql://dbname_rfmr_user:umbipbO7aaO9xEyFqnq92lhttXrkEEdl@dpg-d2og7mffte5s738963kg-a.oregon-postgres.render.com/dbname_rfmr"

#Criação do engine e sessão
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
Base = declarative_base()

# Definição do modelo de dados
class BitcoinDados(Base):
    __tablename__ = "bitcoin_dados"
    
    id = Column(Integer, primary_key=True)
    valor = Column(Float)
    criptomoeda = Column(String(10))
    moeda = Column(String(10))
    timestamp = Column(DateTime)

def extrair():
    url = "https://api.coinbase.com/v2/prices/spot"
    response = requests.get(url)
    return response.json()

def transformar(dados):
    valor = float (dados['data']['amount'])
    criptomoeda = dados['data']['base']
    moeda = dados['data']['currency']
    
    dados_tratados = { 
      "valor": valor, 
      "criptomoeda": criptomoeda,
      "moeda": moeda, 
      "timestamp": datetime.now().strftime("%d/%m/%Y, %H:%M:%S")
      }
    return dados_tratados

def salvar_dados_sqlalchemy(dados):
    """Salva os dados no PostgreSQL usando SQLAlchemy."""
    with Session() as session:
        session.add(dados)
        session.commit()
        print("Dados salvos no PostgreSQL!")

def load(dados_tratados):
    db = TinyDB('db.json')
    db.insert(dados_tratados)
    print("Dados salvos com sucesso!")


if __name__ == "__main__":
    while True: 
        dados_extraidos = extrair()
        tratados = transformar(dados_extraidos)
        load(tratados)
        sleep(5)
    
    
    
