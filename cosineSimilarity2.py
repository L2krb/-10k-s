import pickle
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import matplotlib.pyplot as plt
import seaborn as sns
import glob

plt.style.use('ggplot')

def compute_cosine_similarity(df_grouped):
    try:
        similarity = cosine_similarity(df_grouped)
        return similarity[0][1]
    except IndexError:
        return None

def load_pickle(file_path):
    with open(file_path, 'rb') as f:
        file= pickle.load(f)
    df = pd.DataFrame.from_dict(file, orient='index').reset_index()
    df.rename(columns={'index': 'doc_id', 0: 'business_des', 1: 'risk_factors'}, inplace=True)
    df['CIK'] = df['doc_id'].apply(lambda x: x.split('data_')[1].split('_')[0])
    df['Year'] = df['doc_id'].apply(lambda x: x.split(r'EDGAR 10-K/')[1].split('\\')[0])
    df['filing_date'] = df['doc_id'].apply(lambda x: x.split('\\')[3].split('_')[0])
    df.drop(columns=['business_des', 'doc_id'], inplace=True)
    return df

def _run_over_year(year):
    folder = r'C:\EXTRACT\0_DATA\0_RAW\10K'
    df0 = load_pickle(fr'{folder}/Business_Risk_Segments_{int(year-1)}.pickle')
    df1 = load_pickle(fr'{folder}/Business_Risk_Segments_{year}.pickle')
    df = pd.concat([df0, df1], axis=0)
    df.sort_values(by=['CIK', 'Year'], inplace=True)
    df.reset_index(drop=True, inplace=True)

    print(f'\t- Creating TF-IDF for year {year}')
    vectorizer = TfidfVectorizer(stop_words='english', max_features=2000)
    tfidf_matrix = vectorizer.fit_transform(df['risk_factors'])
    df_tfidf = pd.DataFrame(tfidf_matrix.toarray(), columns=vectorizer.get_feature_names_out())
    text_columns = vectorizer.get_feature_names_out()
    df_tfidf['CIK'] = df['CIK']
    df_tfidf['Year'] = df['Year']
    df_tfidf['filing_date'] = df['filing_date']

    print(f'\t- Computing cosine similarity for year {year}')
    df_similarity = df_tfidf.groupby(['CIK'])[text_columns].apply(compute_cosine_similarity)
    df_similarity = df_similarity.reset_index()
    df_similarity.rename(columns={0: 'cosine_similarity'}, inplace=True)
    df_similarity['year'] = year
    df_similarity = pd.merge(df_similarity, df1[['CIK', 'filing_date']], on='CIK', how='left')

    folder_save = r"C:\Users\Windows 11\Documents\my project\cosine similarities result"
    df_similarity.to_csv(rf'{folder_save}/cosine_similarity_{year}.csv', index=False)


def _run_all():
    for year in range(2021, 2024):
        print(f'Running for year {year}')
        _run_over_year(year)

def _plot():
    folder = r"C:\Users\Windows 11\Documents\my project\cosine similarities result"
    files = glob.glob(f"{folder}/cosine_similarity_*.csv")
    print(f'Found {len(files)} files')
    df = pd.concat([pd.read_csv(file) for file in files], axis=0)
    df.reset_index(drop=True, inplace=True)
    df_pos  =df[df['cosine_similarity']>0.5]

    fig, ax = plt.subplots(figsize=(12, 8))
    sns.histplot(df_pos, x='cosine_similarity', bins=50, ax=ax)
    ax.set_xlabel('Cosine Similarity', fontsize=18)
    ax.set_ylabel('Frequency', fontsize=18)
    plt.savefig('./histogram.pdf', bbox_inches='tight', dpi=150)
    plt.show()


