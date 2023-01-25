class Quote():

    def __init__(self, quote_id, source, text, date, discourse, url, art_title, origin_id):

        self.source = source
        self.text = text
        self.date = date
        self.sid = quote_id
        self.discourse = discourse
        self.url = "/".join(url) if isinstance(url, list) else url
        self.art_title = art_title
        self.origin_id = origin_id