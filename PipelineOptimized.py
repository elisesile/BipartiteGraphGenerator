import ijson
import re
import csv
import pandas as pd
import numpy as np
from datetime import timedelta
from rapidfuzz import fuzz
from DataStructures import Quote, BipartiteGraph, QuoteCluster

import logging

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(name)s - %(message)s', level=logging.INFO)
logger = logging.getLogger("pipeline")

class Pipeline():

    def __init__(self, filename, speaker, quotesOutFile, t=7, partial_matching_score_threshold=85,
                 cluster_belonging_common_string_min_len=35, quotes_already_extracted=False, chunksize=1000, debug=False):

        self.debug = debug
        if self.debug:
            logger.info("Debug activated")

        self.filename = filename
        self.discourses = self.get_discourses(speaker)
        self.quotes_out_file = quotesOutFile

        self.quotes = {}
        self.quote_clusters = []
        self.graph = BipartiteGraph.Graph()
        
        if not quotes_already_extracted:
            self.going_through_file()
      
        self.compare_quotes_and_discourses(t, partial_matching_score_threshold, chunksize)
        self.define_quote_clusters(cluster_belonging_common_string_min_len)
        self.to_csv(speaker)

    def get_discourses(self, speaker):

        discourses = pd.read_csv("Data/" + speaker + 'Speeches.csv')
        discourses['date'] = pd.to_datetime(discourses.date, format='%Y-%m-%d', errors='coerce')
        discourses.dropna(subset=["date"], inplace=True)
        return discourses

# ~*~*~**~*FETCHING QUOTES OUT OF PRESS DATA*~**~*~*~*~

    def going_through_file(self):

        with open(self.filename,"r") as f, open(self.quotes_out_file,"w") as o:
                
                writer = csv.writer(o)
                writer.writerow(["quote_id","native_id","doc_time","media","title","quote","urls"])
                counter = 0

                for line in ijson.items(f, '', multiple_values=True):
                    counter +=1
                    if counter % 1000 == 0 and self.debug:
                        logger.info(f'going_through_file - {counter}')

                    if "document" in line:
                        quotes = re.split('\"|\»|\«', "a" + line['document'])[1::2]
                        i = 0

                        for quote in quotes:

                            if len(quote) > 35 :

                                try:
                                    url = line['urls']
                                except Exception as e:
                                    url = ""
                                try:
                                    media = line['media']
                                except Exception as e:
                                    media = ""

                                writer.writerow([str(i)+"_"+str(line['id']),line['nativeId'],line['docTime'],media,line['title'],quote,url])
                                i += 1

# ~*~*~**~*MATCHING QUOTES AND DISCOURSES*~**~*~*~*~

    def compare_quotes_and_discourses(self, t , partial_matching_score_threshold, chunksize):

        df_iter = pd.read_csv(self.quotes_out_file, chunksize=chunksize, iterator=True)

        for iter_num, chunk in enumerate(df_iter, 1):
            logger.info(f'Processing iteration {iter_num}')

            chunk['doc_time'] = pd.to_datetime(chunk.doc_time, format='%Y-%m-%d', errors='coerce')
            chunk['disc_id'] = chunk.apply(lambda x:self.attribute_quote_to_discourse(x.quote, x.doc_time, t, partial_matching_score_threshold), axis=1)
            chunk = chunk.loc[chunk.disc_id != "default"]

            #saving chunk
            if iter_num == 1 :
                chunk.to_csv("quotes_disc.csv", header=True)
            else :
                chunk.to_csv("quotes_disc.csv", mode="a", header=False)

    def attribute_quote_to_discourse(self, quote_text, quote_date, t, threshold):

        discourses = self.discourses.loc[((self.discourses.date) >= np.datetime64((quote_date - timedelta(days=t)).to_pydatetime())) & ((self.discourses.date) <= np.datetime64(quote_date.to_pydatetime()))]
        discourses.loc[:, "NWscore"] = discourses['content'].apply(lambda x:fuzz.partial_ratio(quote_text, x))

        if discourses.NWscore.max() > threshold :
            id_disc = discourses['NWscore'].idxmax()
            return id_disc
        else:
            return "default"
        
# ~*~*~**~*CREATING QUOTE CLUSTERS*~**~*~*~*~
        
    def define_quote_clusters(self, cluster_belonging_common_string_min_len):
        
        quotes = pd.read_csv("quotes_disc.csv")

        for discourse_id in pd.unique(quotes.disc_id):
            
            quotes_to_consider = quotes.loc[quotes.disc_id == discourse_id]
            n_quote_clusters = 0 

            for num, quote in quotes_to_consider.iterrows():

                is_assigned = False
                
                quote = Quote.Quote(str(discourse_id)+"_"+str(num), quote.media, quote.quote, quote.doc_time, discourse_id, quote.urls, quote.title, quote.native_id)
                self.quotes[num] = quote
                
                if n_quote_clusters != 0 :
                    for cluster in self.quote_clusters[-n_quote_clusters:]:
                        if cluster.is_new_quote_in_cluster(quote, cluster_belonging_common_string_min_len):
                            cluster.add_quote(quote)
                            self.graph.add_edge(quote, cluster)
                            is_assigned = True
        
                if not is_assigned:
                    self.quote_clusters.append(QuoteCluster.QuoteCluster(len(self.quote_clusters), quote, quote.discourse))
                    self.graph.add_edge(quote, self.quote_clusters[-1])
                    n_quote_clusters += 1

    def to_csv(self, speaker):
        
        self.graph_to_csv(speaker)
        self.clusters_to_csv(speaker)
        logger.info("Your files are being written in the Results/ folder, under names starting with %s", speaker)

    def graph_to_csv(self, speaker):
        
        with open("Results/" + speaker + "_graph.csv","w") as f :
            f.write("%s,%s\n"%('source','cluster_id'))
            for key in self.graph.edges.keys():
                f.write("%s,%s\n"%(key,self.graph.edges[key]))
                
    def clusters_to_csv(self, speaker):
        
        with open("Results/" + speaker + "_clusters.csv","w") as file :
            writer = csv.writer(file)
            writer.writerow(['cluster_id','discourse_id','match','#quotes', "urls", "titles", "dates", "ids"])
            for cluster in self.quote_clusters:
                if len(cluster.quotes) > 1 :
                    writer.writerow([cluster.identifier, cluster.discourse, '"' + str(cluster.match) + '"', len(cluster.quotes), "/".join(cluster.urls), '"' + "/".join(cluster.art_title) + '"', "/".join(cluster.art_date), "/".join(cluster.quotes_id)])
                else :
                    writer.writerow([cluster.identifier, cluster.discourse, '"' + str(cluster.quotes[0].text) + '"', len(cluster.quotes), "/".join(cluster.urls), '"' + "/".join(cluster.art_title) + '"', "/".join(cluster.art_date), "/".join(cluster.quotes_id)])