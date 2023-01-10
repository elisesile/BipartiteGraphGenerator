class Graph():

    def __init__(self):

        self.edges = {}

    def add_edge(self, quote, quote_cluster):

        self.edges[quote.source].append(quote_cluster.id)