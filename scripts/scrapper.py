import requests
from datetime import date
from bs4 import BeautifulSoup
from pymongo import MongoClient
import numpy as np
from tqdm import tqdm
import re
import os
import asyncio
import aiohttp
import time
import random

from dotenv import find_dotenv, load_dotenv
load_dotenv(find_dotenv())

# Set the current year and the limit year (5 years ago)
CURRENT_YEAR = date.today().year
LIMIT_YEAR = CURRENT_YEAR - 5

# Loop pelas páginas da lista de filmes
PAGE_URL = f"https://www.imdb.com/search/title/?title_type=feature&release_date={LIMIT_YEAR},{CURRENT_YEAR}"

def conect_db():
    # Conecta-se ao banco de dados "imdb"
    print('Saving data to database...')

    mongodb_uri = os.getenv('MONGODB_URI')
    if not mongodb_uri:
        raise ValueError("Environment variable MONGODB_URI was not found")
    
    client = MongoClient(mongodb_uri)
    
    #create DB
    db = client['imdb_5']

    # Verifica se a coleção "movies" já existe
    if 'movies' not in db.list_collection_names():
        # Se a coleção "movies" não existir, cria-a
        db.create_collection('movies')
        # Cria o índice único na chave "title" da coleção "movies"
        db['movies'].create_index([('title', 1)], unique=True)
    return db

# Função para inserir um filme no banco de dados
def insert_movie(db, variable):
    
    for row in variable:
        movie_data = {
            'title':row[0],
            'year':row[1],
            'genres':row[2],
            'duration':row[3],
            'directed_by':row[4],
            'cast':row[5],
            'reviews':row[6],
            'certificates':row[7],
            'rating':row[8],
            'votes':row[9],
        }
        db.movies.update_one({'title': variable[0]}, {'$set': movie_data}, upsert=True)

def get_cast_info(cast_url):
    response = requests.get(cast_url)
    soup = BeautifulSoup(response.content, "html.parser")
    cast_list = soup.select("table.cast_list tr")
    
    cast = []
    
    for row in cast_list:
        if row.select_one("td.primary_photo"):
            actor_tag = row.select_one("td:nth-of-type(2) a")
            character_tag = row.select_one("td.character")
            if actor_tag and character_tag:
                actor = actor_tag.text.strip()
                character = character_tag.text.strip()
                actor_data = {'name': actor, 'character': character}
                cast.append(actor_data)
   
    return cast

def get_movie(title_tag):
       
    title = title_tag.text.strip()
    
    return title

def get_rating(tag):
          
    rating_tag = tag.select_one('.ratings-imdb-rating')
    rating = rating_tag.get_text().strip() if rating_tag else np.nan

    return float(rating)

def get_certificate_tag(tag):    

    certificate_tag = tag.select_one('.certificate')
    certificates = certificate_tag.get_text().strip() if certificate_tag else np.nan
      
    return certificates

def get_votes(tag):
                
    votes_tag = tag.select_one('.sort-num_votes-visible span:nth-of-type(2)')
    votes_text = votes_tag.text.strip().replace(',', '') if votes_tag else ''
    votes = int(votes_text) if votes_text else np.nan

    return votes

def get_year(tag):
                
    year_tag = tag.select_one(".lister-item-year")
    year = year_tag.text.strip().replace("(", "").replace(")", "") if year_tag else np.nan
    
    pattern = r"\d{4}"  # regex para encontrar quatro dígitos consecutivos
    year_f = re.search(pattern, year)

    return int(year_f.group())

def get_genres(tag):
    genres_tag = tag.select_one(".genre")
    genres = genres_tag.text.strip().replace("\n", "").split(",") if genres_tag else None
    
    if genres:
        genres_without_spaces = [item.strip() for item in genres]
    else:
        genres_without_spaces = []

    return genres_without_spaces

def get_duration(tag):  
    duration_tag = tag.select_one("span.runtime")
    duration = duration_tag.text.strip() if duration_tag else np.nan  
    return duration


def get_directors(tag):
    directors = [director.text for director in tag.select('div.lister-item-content > p:nth-of-type(3) > a')] 
    return directors

def get_cast(title_tag):     
    cast = get_cast_info("https://www.imdb.com/" + title_tag['href'] + "fullcredits")
    return cast


def get_reviews(title_tag):
    reviews_url = f"https://www.imdb.com/{title_tag['href']}reviews"
    reviews_response = requests.get(reviews_url)
    reviews_soup = BeautifulSoup(reviews_response.content, "html.parser")
    reviews_list = reviews_soup.select("div.text.show-more__control")

    reviews = [review.text.strip() for review in reviews_list]
    return reviews

async def fetch(session, url):
    async with session.get(url) as response:
        return await response.text()

async def imdb_collect(db, page_url):
    if page_url is None:
        raise ValueError("PAGE_URL not found")
    
    while True:
            try:
                async with aiohttp.ClientSession() as session:
                    html = await fetch(session, page_url)
                    soup = BeautifulSoup(html, "lxml")

                    movie_tags = soup.select("div.lister-item.mode-advanced")
                    movies_to_insert = []
                    for tag in tqdm(movie_tags):
                        title_tag = tag.select_one("h3.lister-item-header a")
                        
                        if not title_tag:
                            continue
                        
                        title = get_movie(title_tag)
                        year = get_year(tag)
                        genres = get_genres(tag)
                        duration = get_duration(tag)
                        directed_by = get_directors(tag)
                        cast = get_cast(title_tag)
                        reviews = get_reviews(title_tag)
                        certificates = get_certificate_tag(tag)
                        rating = get_rating(tag)
                        votes = get_votes(tag)

                        movies_to_insert.append((title, year, genres, duration, directed_by, cast, reviews, certificates, rating, votes))

                        insert_movie(db, movies_to_insert)

                    print(f"Página {page_url} - As informações foram inseridas com sucesso!")

                    next_button = soup.select_one("a.lister-page-next.next-page")
                    if next_button:
                        page_url = "https://www.imdb.com" + next_button['href']
                    else:
                        break

                    # Esperar um tempo aleatório entre as solicitações
                    await asyncio.sleep(random.uniform(5, 15))

            except ConnectionError as e:
                print("Erro de conexão: ", e)
                print("Aguardando 5 minutos e tentando novamente...")
                await asyncio.sleep(3000)
                continue

'''Function to carry out the main program'''
def main():
    print('Getting data from IMDB website...')

    db = conect_db()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(imdb_collect(db, PAGE_URL))

if __name__ == "__main__":
    main() 