import requests
from tinydb import TinyDB
from sqlalchemy import create_engine, Column, String, Float, Integer, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime
from time import sleep
from dotenv import load_dotenv
import os

load_dotenv()

# Configurações do banco de dados
DATABASE_URL = os.getenv("DATABASE_URL")

# Criação do engine e sessão
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

# Criar a tabela no banco de dados
Base.metadata.create_all(engine)

def extrair():
    url = "https://api.coinbase.com/v2/prices/spot"
    response = requests.get(url)
    response.raise_for_status()  # Verifica se a requisição foi bem-sucedida
    return response.json()

def transformar(dados):
    valor = float(dados['data']['amount'])
    criptomoeda = dados['data']['base']
    moeda = dados['data']['currency']
    timestamp = datetime.now()
    
    # Dados para SQLAlchemy (com timestamp como objeto datetime)
    dados_sqlalchemy = {
        "valor": valor,
        "criptomoeda": criptomoeda,
        "moeda": moeda,
        "timestamp": timestamp
    }
    
    # Dados para TinyDB (com timestamp como string)
    dados_tinydb = {
        "valor": valor,
        "criptomoeda": criptomoeda,
        "moeda": moeda,
        "timestamp": timestamp.strftime("%d/%m/%Y, %H:%M:%S")
    }
    
    return dados_sqlalchemy, dados_tinydb

def salvar_dados_sqlalchemy(dados):
    """Salva os dados no PostgreSQL usando SQLAlchemy."""
    with Session() as session:
        novo_dado = BitcoinDados(
            valor=dados['valor'],
            criptomoeda=dados['criptomoeda'],
            moeda=dados['moeda'],
            timestamp=dados['timestamp']
        )
        session.add(novo_dado)
        session.commit()
        print("Dados salvos no PostgreSQL!")

def load(dados_tinydb):
    """Salva os dados no TinyDB."""
    db = TinyDB('db.json')
    db.insert(dados_tinydb)
    print("Dados salvos no TinyDB!")

if __name__ == "__main__":
    while True:
        try:
            dados_extraidos = extrair()
            dados_sqlalchemy, dados_tinydb = transformar(dados_extraidos)
            load(dados_tinydb)  # Salva no TinyDB
            salvar_dados_sqlalchemy(dados_sqlalchemy)  # Salva no PostgreSQL
            sleep(5)
        except Exception as e:
            print(f"Erro: {e}")
            sleep(5)