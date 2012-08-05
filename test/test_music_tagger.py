import unittest
import music_map_db
import os
import shutil
import music_tagger


class MusicTaggerTest(unittest.TestCase):

    def setUp(self):
        music_map_db.create_dbs()

    def tearDown(self):
        shutil.rmtree(r'c:\_src\music_tagger\test\__pycache__')
        os.remove('music_tagger.log')
        os.remove('unknown_error.log')
        os.remove('unparseable.log')
        os.remove('music_map.sqlite')

    def test(self):
        params = {'playlist_loc': os.path.abspath(r'c:\_src\music_tagger\test\data\test_simple_playlist.m3u8'),
                  'rating': 1,
                  'tags': ['tag1'],
                  'music_root': '.',
                  'db_loc': os.path.abspath(r'c:\_src\music_tagger\test\music_map.sqlite'),
                  'debug': False} 
        music_tagger.MusicTagger(params)
        pass
        


if __name__ == "__main__":
    unittest.main()