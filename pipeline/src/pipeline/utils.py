import gensim
import numpy as np
import pandas as pd
from gensim.parsing.preprocessing import STOPWORDS
from nltk.stem import WordNetLemmatizer, SnowballStemmer
from nltk.stem.porter import *
np.random.seed(2018)
stemmer = PorterStemmer()
from spellchecker import SpellChecker
spell = SpellChecker()
STOPWORDS = list(STOPWORDS)
STOPWORDS.append('covid')
STOPWORDS.append('vaccine')
STOPWORDS.append('vaccines')
STOPWORDS.append('vaccinated')
STOPWORDS.append('bakuna')
STOPWORDS.append('philippines')
STOPWORDS.append('filipino')
STOPWORDS.append('says')
STOPWORDS.append('czar')
STOPWORDS.append('because')
STOPWORDS.append('like')
STOPWORDS.append('get')
import preprocessor as tp


class BreakIt(Exception):
    pass


def lemmatize_stemming(text):
    return stemmer.stem(WordNetLemmatizer().lemmatize(text, pos='v'))


def preprocess(text):
    result = []
    token_list = []
    for token in gensim.utils.simple_preprocess(text):
        if len(token) > 2 and 'haha' not in token and token not in STOPWORDS:
            # fix misspelling
            # misspelled = spell.unknown([token])
            # if misspelled:
            #     for word in misspelled:
            #         token = spell.correction(word)
            result.append(lemmatize_stemming(token))
            token_list.append(token)
    return result, dict(zip(result, token_list))


def produce_mapping(mapping_list):
    mapping_pairs = pd.concat([pd.DataFrame([(k, v) for k, v in d.items()]) for d in mapping_list])
    mapping_pairs['count'] = 1
    mapping121 = mapping_pairs.groupby(by=[0, 1]).count().reset_index().sort_values(by=[0, 'count'], ascending=False).groupby(by=0).head(1)
    mapping12many = mapping_pairs.drop(columns=['count']).drop_duplicates()
    return mapping121, mapping12many


def dummy_score(text, topic):
    keywords = [t[0] for t in topic]
    weights = [t[1] for t in topic]
    weights = [t/max(weights) for t in weights]
    score = 0
    for keyword, weight in zip(keywords, weights):
        if keyword in text:
            score += weight
    return score