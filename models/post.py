class Post:
    def __init__(self, title, content, url):
        self.title = title
        self.content = content
        self.url = url

    @staticmethod
    def get_csv_headers():
        return ['title', 'content', 'url']

    def get_list_for_csv(self):
        list_row = [self.title, self.content, self.url]
        return list_row

    def set_title(self, title):
        self.title = title

    def get_title(self):
        return self.title

    def set_content(self, content):
        self.content = content

    def get_content(self):
        return self.content

    def set_url(self, url):
        self.url = url

    def get_url(self):
        return self.url
