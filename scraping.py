from bs4 import BeautifulSoup
from requests import get
import pandas as pd
from multiprocessing import Pool
import time

def process_link(link : str):
    df = pd.DataFrame(columns=[
        'Title'   ,
        'Genres'  ,
        'Ratings' ,
        'Director',
    ])
    response = get(link)
    print(link)
    soup = BeautifulSoup(response.text, 'html.parser')
    movie_containers    = soup.find_all('div', class_ = 'lister-item mode-advanced') # this is where all the results are
    for i in range(50):
        try : 
            movie_content       = movie_containers[i].find('div', class_ = 'lister-item-content')
            movie_title         = movie_content.a.text.strip()
            movie_genres        = movie_content.find('span', class_ = 'genre').text.strip()
            movie_ratings       = movie_content.find('div', class_ = 'ratings-bar').find('div', 'inline-block ratings-imdb-rating').strong.text.strip()
            movie_people        = movie_content.find('p', class_ = '')
            if 'Director' in movie_people.text:
                movie_director  = movie_people.find_all('a')[0].text.strip()
            else:
                movie_director  = None

            results_dict = {
                'Title'     : movie_title,
                'Genres'    : movie_genres,
                'Ratings'   : movie_ratings,
                'Director'  : movie_director,
            }
            df = pd.concat([df, pd.DataFrame([results_dict])], ignore_index=True)
        except Exception as e:
            print(e)
            print('info get failed for URL: ' + link + '\nAt index: ' + str(i))
    return df

def main(test : bool, samples : int):
    if test:
        start_time = time.time()
        results = []
        num_samples = 1000
        with Pool(10) as p:
            results.append(p.map(process_link, ['https://www.imdb.com/search/title?release_date=2021&sort=num_votes,desc&start=' + str(j*50 + 1) for j in range(int(num_samples/50))]))
        print("--- %s seconds with multiprocessing---" % (time.time() - start_time))

        start_time = time.time()
        results = []
        num_samples = 1000
        results = [process_link(l) for l in ['https://www.imdb.com/search/title?release_date=2021&sort=num_votes,desc&start=' + str(j*50 + 1) for j in range(int(num_samples/50))]]
        print("--- %s seconds on single thread---" % (time.time() - start_time))
    else:
        results = []
        num_samples = samples
        with Pool(10) as p:
            results += (p.map(process_link, ['https://www.imdb.com/search/title?release_date=2021&sort=num_votes,desc&start=' + str(j*50 + 1) for j in range(int(num_samples/50))]))
        return results 

if __name__ == '__main__':
    df = pd.DataFrame(columns=[
        'Title'   ,
        'Genres'  ,
        'Ratings' ,
        'Director',
    ])
    results = main(False, 100000)
    result_df = pd.concat(results, ignore_index=True)
    result_df.to_csv('movie_data.csv')
    result_df.to_pickle('movie_dataframe')
    print(result_df.head())
    print(result_df.shape)
