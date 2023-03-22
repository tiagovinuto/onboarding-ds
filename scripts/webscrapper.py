import psycopg2
import requests
from bs4 import BeautifulSoup

# Define as configurações do banco de dados
conn = psycopg2.connect(
    database="imdb",
    user="postgres",
    password="123456",
    host="localhost",
    port="5432"
)

# Cria um cursor para executar as operações no banco de dados
cur = conn.cursor()

# Define a URL da página do IMDb que queremos analisar
url = "https://www.imdb.com/chart/top"

# Faz uma requisição HTTP para a URL e armazena a resposta na variável 'response'
response = requests.get(url)

# Cria um objeto 'BeautifulSoup' a partir do conteúdo da resposta HTTP
soup = BeautifulSoup(response.content, "html.parser")

# Localiza a tabela que contém os dados que queremos coletar
table = soup.find("tbody", {"class": "lister-list"})

# Localiza todas as linhas da tabela
rows = table.find_all("tr")

# Itera sobre as linhas da tabela
for row in rows:
    # Localiza as colunas que contém o título e a classificação do filme
    title_col = row.find("td", {"class": "titleColumn"})
    rating_col = row.find("td", {"class": "ratingColumn"})

    # Extrai o título, o ano e a classificação do filme das tags HTML correspondentes
    title = title_col.find("a").text
    year = title_col.find("span", {"class": "secondaryInfo"}).text.strip("()")
    rating = rating_col.find("strong").text

    # Insere os dados do filme no banco de dados
    cur.execute("INSERT INTO movie (titulo, ano, ratings) VALUES (%s, %s, %s);", (title, year, rating))

# Salva as alterações no banco de dados
conn.commit()

# Fecha a conexão com o banco de dados
cur.close()
conn.close()
