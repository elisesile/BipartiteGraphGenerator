from fuzzywuzzy import fuzz
from difflib import SequenceMatcher

class QuoteCluster():

    def __init__(self, id, initial_quote, discourse):

        self.quotes = []
        self.quotes.append(initial_quote)
        self.discourse = discourse
        self.identifier = id
        self.match = ""

    def add_quote(self, quote):

        self.quotes.append(quote)

    def add_match(self, match):

        if len(match) > len(self.match) : 
            self.match = match

    def is_new_quote_in_cluster(self, quote, common_string_min_len):

        if quote.date < self.discourse.date:
            return False

        for quotes in self.quotes :

            seqMatch = SequenceMatcher(None,quote.text,quotes.text, False) 
            match = seqMatch.find_longest_match(0, len(quote.text), 0, len(quotes.text)) 

            if match.size > common_string_min_len :
                print ("Common Substring ::>",quote.text[match.a: match.a + match.size])
                self.add_match(quote.text[match.a: match.a + match.size])
                return True

        return False

    