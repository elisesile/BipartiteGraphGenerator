from DataStructures import Quote, BipartiteGraph, QuoteCluster
import pandas as pd
import re
from datetime import datetime, timedelta
from fuzzywuzzy import fuzz
import numpy as np

#A Pipeline for the bipartite graph constructor#

class Pipeline():

    def __init__(self, filename, speaker_of_discourses):
        
        self.quotes = {}
        self.quote_clusters = []
        self.graph = BipartiteGraph.Graph()

        self.articles = pd.read_json("Data/" + filename, lines='true')

        self.discourses = pd.read_csv("Data/" + speaker_of_discourses + 'Speeches.csv')
        self.discourses['date'] = pd.to_datetime(self.discourses.date, format='%Y-%m-%d', errors='coerce')
        self.discourses.dropna(subset=["date"], inplace=True)

        self.data_cleaning()
        self.add_discourses_clean_discourseless()

    def data_cleaning(self):

        print('Careful, this step is to adapt to the speaker. \nCurrent speaker is François Hollande')

        self.articles = self.articles.loc[~ self.articles["document"].str.contains('hollandais|en Hollande')].loc[self.articles['docTime'] > "1995-01-01"] # First discourse by F. Hollande in 1995

        self.articles['splitted_quotes'] = self.articles.document.apply(lambda x : re.split('\"|\»|\«', x)[1::2]) # Split on "" and select only odd index returns, even being text around bracketed text

        self.articles = self.articles.explode('splitted_quotes').reset_index(drop=True).drop(['nativeId','originalDocTime','grabTime','document','urls','tweets', 'docTimeType', 'docTimeModified'], axis=1) # Create a line for each quote identified - duplicating the info of the article
        self.articles = self.articles.loc[self.articles.splitted_quotes.str.len() > 35] # Filter by quote length, uninterested by quotes under 35 characters 

        self.articles['docTime'] = pd.to_datetime(self.articles.docTime, format='%Y-%m-%d', errors='coerce')
        self.articles.dropna(subset=['docTime'], inplace=True)
        self.articles.dropna(subset=['splitted_quotes'], inplace=True)

    def add_discourses_clean_discourseless(self):

        self.articles['disc_id'] = self.articles.apply(lambda x:self.attribute_quote_to_discourse(x.splitted_quotes, x.docTime), axis=1)
        print(self.articles)
        self.articles = self.articles.loc[self.articles.disc_id != "default"]

    def attribute_quote_to_discourse(self, quote_text, quote_date, t=7, threshold=55):

        discourses = self.discourses.loc[((self.discourses.date) >= np.datetime64((quote_date - timedelta(days=t)).to_pydatetime())) & ((self.discourses.date) <= np.datetime64(quote_date.to_pydatetime()))]
        discourses['NWscore'] = discourses['content'].map(lambda x : fuzz.partial_ratio(x, quote_text))

        if discourses.NWscore.max() > threshold :
            id_disc = discourses['NWscore'].idxmax()
            del discourses
            return id_disc
        else:
            del discourses
            return "default"

    def define_quote_clusters(self, quotes):
        
        for discourse_id in pd.unique(quotes.disc_id):
            
            quotes_to_consider = quotes.loc[quotes.disc_id == discourse_id]
            n_quote_clusters = 0
            
            for num, quote in quotes_to_consider.iterrows():

                print(quote)
                
                quote = Quote.Quote(str(discourse_id)+"_"+str(num), quote.media + "/" + quote.author, quote.splitted_quotes, quote.docTime, discourse_id)
                self.quotes[num] = quote
                
                if n_quote_clusters != 0 :
                    for cluster in self.quote_clusters[-n_quote_clusters:]:
                        if cluster.is_new_quote_in_cluster(quote):
                            cluster.add_quote(quote)
                            self.set_edge(quote, cluster)
                            print("Quote belongs to quote cluster", cluster.id)
        
                else:
                    self.quote_not_in_existing_cluster(quote)
                    n_quote_clusters += 1
                    
            print("Treated discourse n°", discourse_id)
    
    def quote_not_in_existing_cluster(self, quote):

        self.quote_clusters.append(QuoteCluster.QuoteCluster(len(self.quote_clusters), quote, quote.discourse))
        return 1