from DataStructures import Quote, BipartiteGraph, QuoteCluster
import pandas as pd
import re, csv
from datetime import datetime, timedelta
from fuzzywuzzy import fuzz
import numpy as np
import logging

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(name)s - %(message)s', level=logging.INFO)
logger = logging.getLogger("pipeline")

#A Pipeline for the bipartite graph constructor#

class Pipeline():

    def __init__(self, filename, speaker_of_discourses, t=7, partial_matching_score_threshold=55,
                 cluster_belonging_common_string_min_len=35, debug=False):
        logger.info("Starting %s speeches on %s", speaker_of_discourses, filename)
        self.debug = debug
        if self.debug:
            logger.info("Debug activated")
        self.quotes = {}
        self.quote_clusters = []
        self.graph = BipartiteGraph.Graph()

        self.articles = pd.read_json(filename, lines='true')
        self.speaker = speaker_of_discourses

        self.discourses = pd.read_csv("Data/" + speaker_of_discourses + 'Speeches.csv')
        self.discourses['date'] = pd.to_datetime(self.discourses.date, format='%Y-%m-%d', errors='coerce')
        self.discourses.dropna(subset=["date"], inplace=True)

        self.data_cleaning()
        logger.info("Articles Cleaned !")
        self.add_discourses_clean_discourseless(t, partial_matching_score_threshold)
        logger.info("Discourses Linked to Quotes !")
        self.define_quote_clusters(self.articles, cluster_belonging_common_string_min_len)
        logger.info("Quote Clusters Created !")
        self.to_csv()

    def data_cleaning(self):

        logger.info('Careful, this step is to adapt to the speaker. Current speaker is %s', self.speaker)

        self.articles = self.articles.loc[self.articles['docTime'] > "2017-01-01"] # First discourse by F. Hollande in 1995

        self.articles['splitted_quotes'] = self.articles.document.apply(lambda x : re.split('\"|\»|\«', x)[1::2]) # Split on "" and select only odd index returns, even being text around bracketed text

        self.articles = self.articles.explode('splitted_quotes').reset_index(drop=True).drop(['originalDocTime','grabTime','document','tweets', 'docTimeType', 'docTimeModified'], axis=1) # Create a line for each quote identified - duplicating the info of the article
        self.articles = self.articles.loc[self.articles.splitted_quotes.str.len() > 35] # Filter by quote length, uninterested by quotes under 35 characters 

        self.articles['docTime'] = pd.to_datetime(self.articles.docTime, format='%Y-%m-%d', errors='coerce')
        self.articles['media'] = self.articles['media'].astype(str)
        self.articles.dropna(subset=['splitted_quotes'], inplace=True)
        self.articles.dropna(subset=['docTime'], inplace=True)

    def add_discourses_clean_discourseless(self, t, partial_matching_score_threshold):
        self.count = 0
        self.articles['disc_id'] = self.articles.apply(lambda x:self.attribute_quote_to_discourse(x.splitted_quotes, x.docTime, x.nativeId, t, partial_matching_score_threshold), axis=1)
        self.articles = self.articles.loc[self.articles.disc_id != "default"]

    def attribute_quote_to_discourse(self, quote_text, quote_date, quote_id, t, threshold):
        self.count += 1
        if self.debug:
            logger.info("  ... article %d / %d - %s", self.count, self.articles.shape[0], quote_id)
        elif self.count % 1000 == 0:
            logger.info("  ... article %d / %d", self.count, self.articles.shape[0])
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
                
                quote = Quote.Quote(str(discourse_id)+"_"+str(num), quote.media, quote.splitted_quotes, quote.docTime, discourse_id, quote.urls, quote.title, quote.nativeId)
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
                    
            logger.info("Treated discourse n° %d", discourse_id)
    
    def quote_not_in_existing_cluster(self, quote):

        self.quote_clusters.append(QuoteCluster.QuoteCluster(len(self.quote_clusters), quote, quote.discourse))
        return 1

    def to_csv(self):
        
        self.graph_to_csv()
        self.clusters_to_csv()
        logger.info("Your files are being written in the Results/ folder, under names starting with %s", self.speaker)

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
                    writer.writerow([cluster.identifier, cluster.discourse, '"' + str(cluster.match) + '"', len(cluster.quotes), "/".join(cluster.urls), '"' + "/".join(cluster.art_title) + '"', "/".join(cluster.art_date), "/".join(cluster.quotes_id)])
                else :
                    writer.writerow([cluster.identifier, cluster.discourse, '"' + str(cluster.quotes[0].text) + '"', len(cluster.quotes), "/".join(cluster.urls), '"' + "/".join(cluster.art_title) + '"', "/".join(cluster.art_date), "/".join(cluster.quotes_id)])