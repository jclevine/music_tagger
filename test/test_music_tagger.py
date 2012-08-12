import unittest
import music_map_db
import os
import music_tagger
import music_map
import test_utils as mm_test_utils
import sqlite3
from util import sqlite_utils


class MusicTaggerTest(unittest.TestCase):
    TEST_LOC = r'c:\_src\music_tagger\test'
    TEST_DATA_LOC = os.path.join(TEST_LOC, 'data')
    TEST_DB_LOC = os.path.join(TEST_LOC, 'test_tagger.sqlite')


    def setUp(self):
        music_map_db.create_dbs(self.TEST_DB_LOC)

    def tearDown(self):
        os.remove('music_tagger.log')
        os.remove('unknown_error.log')
        os.remove('unparseable.log')
        os.remove('music_map.log')
        os.remove(self.TEST_DB_LOC)

    def test_simple(self):
        try:
            # First get all the songs into the song/music_map DB.
            song_source_playlist = os.path.join(self.TEST_DATA_LOC, 'test_simple_playlist_source.m3u8')
            music_map_params = {'playlist_loc': song_source_playlist,
                                'music_roots': ['./somewhere_else'],
                                'db_loc': self.TEST_DB_LOC,
                                'debug': False}
            music_map.MusicMap(music_map_params)
            tagger_params = {'playlist_loc': os.path.join(self.TEST_DATA_LOC, 'test_simple_playlist.m3u8'),
                             'rating': 1,
                             'tags': ['tag1'],
                             'music_roots': ['.'],
                             'db_loc': self.TEST_DB_LOC,
                             'debug': False}
            music_tagger.MusicTagger(tagger_params)

            # TODO: !3 Reuse music map's insert into music map test util
            # TODO: !3 Have util functions to get all rows of all possible tables.
            conn = sqlite3.connect(self.TEST_DB_LOC)
            song_cursor = conn.cursor()
            song_query = """
                    SELECT artist_key
                         , album_key
                         , track_key
                         , title_key
                      FROM song
                    """
            song_rs = song_cursor.execute(song_query)
            song_rows = sqlite_utils.name_columns(song_rs)
            for row in song_rows:
                self.assertEqual('artist', row['artist_key'])
                self.assertEqual('album', row['album_key'])
                self.assertEqual('01', row['track_key'])
                self.assertEqual('track title', row['title_key'])

            mm_cursor = conn.cursor()
            mm_query = """
                    SELECT song_id
                         , location
                         , artist
                         , album
                         , track
                         , title
                         , full_path
                      FROM music_map
                      """
            mm_rs = mm_cursor.execute(mm_query)
            mm_rows = sqlite_utils.name_columns(mm_rs)
            self.assertEquals(1, len(mm_rows))
            song_id = mm_rows[0]['song_id']

            tag_cursor = conn.cursor()
            tag_query = """
                        SELECT tag
                          FROM music_tag mt
                         WHERE mt.song_id = ?
                 """
            tag_values = (str(song_id))
            tag_cursor.execute(tag_query, tag_values)
            row = tag_cursor.fetchone()
            self.assertEqual('tag1', row[0])

        finally:
            conn.close()

if __name__ == "__main__":
    unittest.main()