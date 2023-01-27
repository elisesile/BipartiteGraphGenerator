from fuzzywuzzy import fuzz
from difflib import SequenceMatcher

class QuoteCluster():

    def __init__(self, id, initial_quote, discourse):

        self.quotes = []
        self.quotes.append(initial_quote)
        self.discourse = discourse
        self.identifier = id
        self.match = ""
        self.urls = [str(initial_quote.url)]
        self.quotes_id = [str(initial_quote.origin_id)]
        self.art_title = [str(initial_quote.art_title)]
        self.art_date = [str(initial_quote.date)]

    def add_quote(self, quote):

        self.quotes.append(quote)
        self.urls.append(str(quote.url))
        self.quotes_id.append(str(quote.origin_id))
        self.art_title.append(str(quote.art_title))
        self.art_date.append(str(quote.date))

    def add_match(self, match):

        if len(match) > len(self.match) : 
            self.match = match

    def is_new_quote_in_cluster(self, quote, common_string_min_len):

        for quotes in self.quotes :

            seqMatch = SequenceMatcher(None,quote.text,quotes.text, False) 
            match = seqMatch.find_longest_match(0, len(quote.text), 0, len(quotes.text)) 

            if match.size > common_string_min_len :
                print ("Common Substring ::>",quote.text[match.a: match.a + match.size])
                self.add_match(quote.text[match.a: match.a + match.size])
                return True

        return False

    