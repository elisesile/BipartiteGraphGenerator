from DataStructures import Quote, BipartiteGraph, QuoteCluster
import pandas as pd
import re, csv
from datetime import datetime, timedelta
from fuzzywuzzy import fuzz
import numpy as np

#A Pipeline for the bipartite graph constructor#

class Pipeline():

    def __init__(self, filename, speaker_of_discourses, t=7, partial_matching_score_threshold=55, cluster_belonging_common_string_min_len=35):
        
        self.quotes = {}
        self.quote_clusters = []
        self.graph = BipartiteGraph.Graph()

        self.articles = pd.read_json(filename, lines='true')
        self.speaker = speaker_of_discourses

        self.discourses = pd.read_csv("Data/" + speaker_of_discourses + 'Speeches.csv')
        self.discourses['date'] = pd.to_datetime(self.discourses.date, format='%Y-%m-%d', errors='coerce')
        self.discourses.dropna(subset=["date"], inplace=True)

        self.data_cleaning()
        print("\nArticles Cleaned !")
        self.add_discourses_clean_discourseless(t, partial_matching_score_threshold)
        print("\nDiscourses Linked to Quotes !")
        self.define_quote_clusters(self.articles, cluster_belonging_common_string_min_len)
        print("\nQuote Clusters Created !")
        self.to_csv()

    def data_cleaning(self):

        print('Careful, this step is to adapt to the speaker. \nCurrent speaker is François Hollande')

        self.articles = self.articles.loc[~ self.articles["document"].str.contains('hollandais|en Hollande')].loc[self.articles['docTime'] > "1995-01-01"] # First discourse by F. Hollande in 1995

        self.articles['splitted_quotes'] = self.articles.document.apply(lambda x : re.split('\"|\»|\«', x)[1::2]) # Split on "" and select only odd index returns, even being text around bracketed text

        self.articles = self.articles.explode('splitted_quotes').reset_index(drop=True).drop(['nativeId','originalDocTime','grabTime','document','urls','tweets', 'docTimeType', 'docTimeModified'], axis=1) # Create a line for each quote identified - duplicating the info of the article
        self.articles = self.articles.loc[self.articles.splitted_quotes.str.len() > 35] # Filter by quote length, uninterested by quotes under 35 characters 

        self.articles['docTime'] = pd.to_datetime(self.articles.docTime, format='%Y-%m-%d', errors='coerce')
        self.articles.dropna(subset=['docTime'], inplace=True)
        self.articles.dropna(subset=['splitted_quotes'], inplace=True)

    def add_discourses_clean_discourseless(self, t, partial_matching_score_threshold):

        self.articles['disc_id'] = self.articles.apply(lambda x:self.attribute_quote_to_discourse(x.splitted_quotes, x.docTime, t, partial_matching_score_threshold), axis=1)
        self.articles = self.articles.loc[self.articles.disc_id != "default"]

    def attribute_quote_to_discourse(self, quote_text, quote_date, t, threshold):

        discourses = self.discourses.loc[((self.discourses.date) >= np.datetime64((quote_date - timedelta(days=t)).to_pydatetime())) & ((self.discourses.date) <= np.datetime64(quote_date.to_pydatetime()))]
        discourses['NWscore'] = discourses['content'].map(lambda x : fuzz.partial_ratio(x, quote_text))

        if discourses.NWscore.max() > threshold :
            id_disc = discourses['NWscore'].idxmax()
            del discourses
            return id_disc
        else:
            del discourses
            return "default"

    def define_quote_clusters(self, quotes, cluster_belonging_common_string_min_len):
        
        for discourse_id in pd.unique(quotes.disc_id):
            
            quotes_to_consider = quotes.loc[quotes.disc_id == discourse_id]
            n_quote_clusters = 0
            
            for num, quote in quotes_to_consider.iterrows():

                is_assigned = False
                
                quote = Quote.Quote(str(discourse_id)+"_"+str(num), quote.media + "/" + quote.author, quote.splitted_quotes, quote.docTime, discourse_id)
                self.quotes[num] = quote
                
                if n_quote_clusters != 0 :
                    for cluster in self.quote_clusters[-n_quote_clusters:]:
                        if cluster.is_new_quote_in_cluster(quote, cluster_belonging_common_string_min_len):
                            cluster.add_quote(quote)
                            self.graph.add_edge(quote, cluster)
                            is_assigned = True
        
                if not is_assigned:
                    self.quote_not_in_existing_cluster(quote)
                    self.graph.add_edge(quote, self.quote_clusters[-1])
                    n_quote_clusters += 1
                    
            print("Treated discourse n°", discourse_id)
    
    def quote_not_in_existing_cluster(self, quote):

        self.quote_clusters.append(QuoteCluster.QuoteCluster(len(self.quote_clusters), quote, quote.discourse))
        return 1

    def to_csv(self):
        
        self.graph_to_csv()
        self.clusters_to_csv()
        print("Your files are being written in the Results/ folder, under names starting with", self.speaker)

    def graph_to_csv(self):
        
        with open("Results/" + self.speaker + "_graph.csv","w") as f :
            f.write("%s,%s\n"%('source','cluster_id'))
            for key in self.graph.edges.keys():
                f.write("%s,%s\n"%(key,self.graph.edges[key]))
                
    def clusters_to_csv(self):
        
        with open("Results/" + self.speaker + "_clusters.csv","w") as file :
            writer = csv.writer(file)
            writer.writerow(['cluster_id','discourse_id','match','#quotes'])
            for cluster in self.quote_clusters:
                if len(cluster.quotes) > 1 :
                    writer.writerow([cluster.identifier, cluster.discourse, cluster.match, len(cluster.quotes)])
                else :
                    writer.writerow([cluster.identifier, cluster.discourse, cluster.quotes[0].text, len(cluster.quotes)])