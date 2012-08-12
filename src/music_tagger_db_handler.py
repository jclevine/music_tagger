# TODO: !3 Go through all queries and make them simpler, perhaps without cursors and rs's.
import logging


class MusicTaggerDBHandler(object):

    def __init__(self, cursor):
        self._cursor = cursor

    def insert_rating_and_tags(self, song_obj, rating, tags):
        song_id = self._get_song_id(song_obj)

        # TODO: !2 Warn, but insert it.
        if not song_id:
            logging.getLogger('music_tagger').warning('Song does not exist in DB: {0}'
                                                      .format(str(song_obj)))
        # TODO: !3 Make function for rating and tags.
        if rating:
            if song_id and not self._song_id_in_rating(song_id):
                query = """
                        INSERT INTO music_rating
                                  ( song_id
                                  , rating
                                  )
                             VALUES
                                  ( ?
                                  , ?
                                  )
                        """
                values = (song_id, rating)
                # TODO: !2 Try/except here.
                self._cursor.execute(query, values)

        # TODO: !3 Make function for rating and tags.
        for tag in tags:
            if not self._song_id_and_tag_in_tags(song_id, tag):
                query = """
                        INSERT INTO music_tag
                                  ( song_id
                                  , tag
                                  )
                             VALUES
                                  ( ?
                                  , ?
                                  )
                        """
                values = (song_id, tag)
                self._cursor.execute(query, values)

    def _get_song_id(self, song_obj):
        query = """
                SELECT s.song_id
                  FROM song s
                 WHERE s.artist_key = ?
                   AND s.album_key  = ?
                   AND s.track_key  = ?
                   AND s.title_key  = ?
                """
        values = (song_obj.artist_key,
                  song_obj.album_key,
                  song_obj.track_key,
                  song_obj.title_key)
        rs = self._cursor.execute(query, values)
        song_id = rs.fetchone()
        return song_id[0] if song_id else None

    def _song_id_in_rating(self, song_id):
        query = """
                SELECT COUNT(*)
                  FROM music_rating mr
                 WHERE mr.song_id = ?
                """
        values = (song_id,)
        rs = self._cursor.execute(query, values)
        num_rows = rs.fetchone()[0]
        return num_rows == 1

    def _song_id_and_tag_in_tags(self, song_id, tag):
        query = """
                SELECT COUNT(*)
                  FROM music_tag mt
                 WHERE mt.song_id = ?
                   AND mt.tag     = ?
                """
        values = (song_id, tag)
        rs = self._cursor.execute(query, values)
        num_rows = rs.fetchone()[0]
        return num_rows == 1

    def close(self):
        self._cursor.close()
