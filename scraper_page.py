import requests
import time
from bs4 import BeautifulSoup
import csv
from tqdm import tqdm
import os
import multiprocessing as mp
import pandas as pd

SCRAPING_DELAY = 0.5

def parse_stars(stars):
    num_stars = 0
    for char in stars:
        if char == '★':
            num_stars += 1
        elif char == '½':
            num_stars += 0.5
    return num_stars

def get_page(url):
    headers = {
        'authority': 'letterboxd.com',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'max-age=0',
        'sec-ch-ua': '"Not_A Brand";v="99", "Microsoft Edge";v="109", "Chromium";v="109"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 Edg/109.0.1518.78',
    }

    response = requests.get(url, headers=headers)
    return response

def get_user_page(username, page_num=1):
    if page_num == 1:
        url = f'https://letterboxd.com/{username}/films/'
    else:
        url = f'https://letterboxd.com/{username}/films/page/{page_num}/'
    return get_page(url)

def get_watchlist_page(username, page_num=1):
    if page_num == 1:
        url = f'https://letterboxd.com/{username}/watchlist/'
    else:
        url = f'https://letterboxd.com/{username}/watchlist/page/{page_num}/'
    return get_page(url)

def get_movie_from_poster_container(poster_container):
    try:
        movie_title = poster_container.select('img')[0]['alt']
    except IndexError:
        movie_title = None
    return movie_title

def get_film_slug_from_poster_container(poster_container):
    try:
        film_slug = poster_container.select('.really-lazy-load')[0]['data-film-slug']
    except IndexError:
        film_slug = None
    return film_slug

def get_movie_id_from_poster_container(poster_container):
    try:
        movie_id = poster_container.select('.really-lazy-load')[0]['data-film-id']
    except IndexError:
        movie_id = None
    return movie_id

def get_movie_rating_from_poster_container(poster_container):
    try:
        movie_rating = parse_stars(poster_container.select('.rating')[0].text)
    except IndexError:
        movie_rating = None
    return movie_rating

def parse_ratings_from_poster_container(poster_container):
    movie_title = get_movie_from_poster_container(poster_container)
    movie_rating = get_movie_rating_from_poster_container(poster_container)
    movie_id = get_movie_id_from_poster_container(poster_container)
    film_slug = get_film_slug_from_poster_container(poster_container)

    return movie_title, movie_rating, movie_id, film_slug

def parse_title_from_watchlist_poster_container(poster_container):
    movie_title = get_movie_from_poster_container(poster_container)
    movie_id = get_movie_id_from_poster_container(poster_container)
    film_slug = get_film_slug_from_poster_container(poster_container)
    return movie_title, movie_id, film_slug

def get_all_user_reviews(username):
    data = []
    page_num = 1
    last_request_time = 10
    while True:
        if (last_request_time - time.time()) < SCRAPING_DELAY:
            time.sleep(SCRAPING_DELAY)
        soup = BeautifulSoup(get_user_page(username, page_num).text, 'html.parser')
        last_request_time = time.time()
        poster_containers = soup.select('.poster-container')
        if not poster_containers:
            break
        for poster_container in poster_containers:
            movie_title, movie_rating, movie_id, film_slug = parse_ratings_from_poster_container(poster_container)
            data.append({
                'movie_title': movie_title,
                'movie_rating': movie_rating,
                'movie_id': movie_id,
                'film_slug': film_slug
            })
        page_num += 1
    return data

def get_watchlist(username):
    data = []
    page_num = 1
    last_request_time = 10
    while True:
        if (last_request_time - time.time()) < SCRAPING_DELAY:
            time.sleep(SCRAPING_DELAY)
        soup = BeautifulSoup(get_watchlist_page(username, page_num).text, 'html.parser')
        last_request_time = time.time()
        poster_containers = soup.select('.poster-container')
        if not poster_containers:
            break
        for poster_container in poster_containers:
            movie_title, movie_id, film_slug = parse_title_from_watchlist_poster_container(poster_container)
            data.append({
                'movie_title': movie_title,
                'movie_id': movie_id,
                'film_slug': film_slug
            })
        page_num += 1
    return data

def get_watchlist_overlap(users):
    watchlists = []
    for user in users:
        watchlists.append(get_watchlist(user))
    
    movie_ids_per_user = []
    for watchlist in watchlists:
        movie_ids_per_user.append(set(movie['movie_id'] for movie in watchlist))
    
    overlap = movie_ids_per_user[0]
    for movie_ids in movie_ids_per_user[1:]:
        overlap = overlap.intersection(movie_ids)
    
    # get the movie titles
    movie_titles = []
    for movie_id in overlap:
        for movie in watchlists[0]:
            if movie['movie_id'] == movie_id:
                movie_titles.append(movie['movie_title'])
                break
    
    return movie_titles

def save_user_reviews(username):
    try:
        if (os.path.exists(f'user_reviews/{username}_reviews.csv')):
            print(f'User reviews for {username} already saved')
            return
        data = get_all_user_reviews(username)
        if (len(data) == 0):
            print(f'No reviews found for {username}')
            return
        # we want to save as a csv
        with open(f'user_reviews/{username}_reviews.csv', 'w', newline='', encoding='UTF-8') as f:
            writer = csv.DictWriter(f, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
    except UnicodeEncodeError:
        print(f'Error saving {username} reviews')

def get_film_page(film_slug):
    url = f'https://letterboxd.com/{film_slug}/'
    return get_page(url)

def get_film_reviews_page(film_slug, page_num=1):
    url = f'https://letterboxd.com/{film_slug}/members/rated/.5-5/by/date/page/{page_num}/'
    return get_page(url)

def get_review_table_rows_from_reviews_page_soup(page_soup):
    return page_soup.select('.person-table')[0].select('tr')

def parse_reviews_from_film_page(film_slug):
    data = []
    last_request_time = 10
    for page_num in tqdm(range(1, 25)):
        if (last_request_time - time.time()) < SCRAPING_DELAY:
            time.sleep(SCRAPING_DELAY)
        last_request_time = time.time()
        soup = BeautifulSoup(get_film_reviews_page(film_slug, page_num).text, 'html.parser')
        review_table_rows = get_review_table_rows_from_reviews_page_soup(soup)[1:] # skip the header row
        for review_table_row in review_table_rows:
            user_data = review_table_row.select('.table-person')[0]
            user_url = user_data.select('.name')[0].get('href')
            user_rating = parse_stars(review_table_row.select('.rating')[0].text)
            data.append({
                'user_url': user_url,
                'user_rating': user_rating,
                'username': user_url.split('/')[1]
            })
        page_num += 1
    return data

def save_reviews_from_film_page(film_slug):
    movie_name = film_slug.split('/')[2]
    if os.path.exists(f'film_reviews/{movie_name}_reviews.csv'):
        return
    film_reviews = parse_reviews_from_film_page(film_slug)
    with open(f'film_reviews/{movie_name}_reviews.csv', 'w') as f:
        writer = csv.DictWriter(f, fieldnames=film_reviews[0].keys())
        writer.writeheader()
        writer.writerows(film_reviews)

def get_starting_user_bfs_queue():
    # read all the files in film_reviews
    film_reviews = []
    for filename in os.listdir('film_reviews'):
        film_reviews.append(pd.read_csv(f'film_reviews/{filename}'))
    film_reviews = pd.concat(film_reviews)
    film_reviews = film_reviews.drop_duplicates(subset=['user_url'])
    film_reviews = film_reviews.dropna()
    return film_reviews['user_url'].tolist()


def get_popular_film_slugs(page_num=1):
    film_slugs = []
    page_data = get_page('https://letterboxd.com/films/popular/page/' + str(page_num) + '/').text
    soup = BeautifulSoup(page_data, 'html.parser')
    

if __name__ == '__main__':
    starting_queue = get_starting_user_bfs_queue()
    print(f'Number of users to scrape: {len(starting_queue)}')
    with mp.Pool(8) as p:
        p.map(save_user_reviews, [user_url.split('/')[1] for user_url in starting_queue])
