import unittest
import requests
import json
import os

def search(query):
    r = requests.post(f"http://{os.getenv('HOST', 'localhost')}:5000/search", json={"query": query})
    return r


class TestSearch(unittest.TestCase):
    def test_bad_request(self):
        r = search(None)
        self.assertEqual(r.status_code, 400, "A bad request should result in a 400")
    
    def test_bad_request_empty_spaces(self):
        r = search('  ')
        self.assertEqual(r.status_code, 400, "A bad request with empty spaces should result in a 400")
    
    def test_no_results(self):
        r = search("Never mind me lalala")
        self.assertEqual([], r.json(), "No search results should be empty")

    def test_title(self):
        r = search("Picard fact")
        self.assertEqual(
            [{"id": 8, "title": "Not Picard! Not anyone, in fact", "content": []}],
            r.json(),
            "Title search should return results",
        )

    def test_toplevel_content(self):
        r = search("enterprise")
        self.assertEqual(
            [
                {
                    "id": 1,
                    "title": "These are the voyages",
                    "content": [
                        {
                            "type": "text",
                            "body": "These are the voyages of the Starship Enterprise",
                        },
                        {
                            "type": "text",
                            "body": " Its continuing mission, to explore strange new worlds",
                        },
                        {
                            "type": "text",
                            "body": " To seek out new life and new civilizations",
                        },
                        {
                            "type": "text",
                            "body": " To boldly go where no one has gone before",
                        },
                        {"type": "text", "body": ""},
                    ],
                }
            ],
            r.json(),
            "Content search should return results",
        )

    def test_toplevel_AND(self):
        r = search("enterprise picard")
        self.assertEqual(
            [], r.json(), "All the text must be found in an answer",
        )

    def test_title_AND_content(self):
        r = search("hails hugh")
        self.assertEqual(
            [
                {
                    "id": 6,
                    "title": "Borg Hails",
                    "content": [
                        {
                            "type": "random",
                            "body": [
                                {
                                    "type": "text",
                                    "body": "You will be assimilated. Resistance is futile.",
                                },
                                {
                                    "type": "text",
                                    "body": "We are the Borg. Lower your shields and surrender your ships. We will add your biological and technological distinctiveness to our own. Your culture will adapt to service us. Resistance is futile.",
                                },
                                {
                                    "type": "text",
                                    "body": "We are the Borg. Your biological and technological distinctiveness will be added to our own. Resistance is futile.",
                                },
                            ],
                        },
                        {
                            "type": "random",
                            "body": [
                                {
                                    "type": "image",
                                    "url": "https://vignette.wikia.nocookie.net/memoryalpha/images/4/48/Picard_dreams_his_assimilation.jpg/revision/latest?cb=20120317185405&path-prefix=en",
                                },
                                {
                                    "type": "image",
                                    "url": "https://vignette.wikia.nocookie.net/memoryalpha/images/4/40/Hugh-Drone.jpg/revision/latest?cb=20141207193522&path-prefix=en",
                                    "alt-text": "Hugh",
                                },
                            ],
                        },
                    ],
                }
            ],
            r.json(),
            "All the text must be found in an answer",
        )

    def test_title_case_insensitive(self):
        r = search("PICARD FACT")
        self.assertEqual(
            [{"id": 8, "title": "Not Picard! Not anyone, in fact", "content": []}],
            r.json(),
            "Title search should be insensitive",
        )

    def test_nested(self):
        r = search("facepalm")
        self.assertCountEqual(
            [
                {
                    "id": 3,
                    "title": "Star Trek API",
                    "content": [
                        {"type": "text", "body": "Api call time!"},
                        {
                            "type": "http",
                            "success": [
                                {"type": "text", "body": "Made it so!"},
                                {
                                    "type": "image",
                                    "url": "https://treknews.net/wp-content/uploads/2011/05/2.jpg",
                                    "alt-text": "picard thumbs up",
                                },
                            ],
                            "failure": [
                                {
                                    "type": "random",
                                    "body": [
                                        {
                                            "type": "image",
                                            "url": "https://d13ezvd6yrslxm.cloudfront.net/wp/wp-content/images/startrek-picard-facepalm-700x341.jpg",
                                            "alt-text": "picard facepalm",
                                        },
                                        {
                                            "type": "image",
                                            "url": "https://www.everseradio.com/wp-content/uploads/2009/06/facepalm.png",
                                            "alt-text": "picard double facepalm",
                                        },
                                    ],
                                }
                            ],
                        },
                    ],
                }
            ],
            r.json(),
            "Nested content should be found",
        )
    #this means we need to get content filed - convert it to json and anulyse it first before returning
    def test_type_should_be_excluded(self):
        r = search("random")
        self.assertEqual(
            [], r.json(), "The 'type' field should be excluded from searches",
        )

    def test_field_names_should_be_excluded(self):
        r = search("alt-text")
        self.assertEqual(
            [], r.json(), "JSON field names should be excluded from searches",
        )

    def test_order_does_not_matter(self):
        r1 = search("before boldly")
        r2 = search("boldly before")

        self.assertEqual(
            r1.json(), r2.json(), "The order or search terms should not matter",
        )



if __name__ == "__main__":
    unittest.main()
