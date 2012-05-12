# TODO: !3 Go through all queries and make them simpler, perhaps without cursors and rs's.
import logging
import difflib

class MusicTaggerDBHandler(object):

    def __init__(self, cursor):
        self._cursor = cursor

    def insert_rating_and_tags(self, song_obj, rating, tags):
        song_id = self._get_song_id(song_obj)

        if not song_id:
            logging.getLogger('music_tagger').warning('Song does not exist in DB: {0}'
                                                      .format(str(song_obj)))
            return

        # TODO: !3 Make function for rating and tags.
        if rating:
            if not self._song_id_in_rating(song_id):
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

        # Get all the songs by artist/track
        query = """
                SELECT s.song_id
                     , s.artist_key
                     , s.album_key
                     , s.title_key
                  FROM song s
                 WHERE s.artist_key = ?
                   AND s.track_key  = ?
                """
        values = (song_obj.artist_key,
                  song_obj.track_key)

        rs = self._cursor.execute(query, values)

        matches = []
        for row in rs:
            matches.append({'song_id': row[0],
                            'artist_key': row[1],
                            'album_key': row[2],
                            'title_key': row[3]})

        for match in matches:
            album_similarity = difflib.SequenceMatcher(None, match['album_key'], song_obj.album_key).ratio()
            title_similarity = difflib.SequenceMatcher(None, match['title_key'], song_obj.title_key).ratio()

            if album_similarity > 0.3 or \
               title_similarity > 0.3 or \
               (album_similarity > 0.1 and title_similarity > 0.1):
                return match['song_id']

        for part_of_artist in song_obj.artist_key.split(' '):

            # Get all the songs by artist/track
            query = """
                    SELECT s.song_id
                         , s.artist_key
                         , s.album_key
                         , s.title_key
                      FROM song s
                     WHERE s.artist_key like ?
                       AND s.track_key  = ?
                    """
            values = ('%' + part_of_artist + '%',
                      song_obj.track_key)

            rs = self._cursor.execute(query, values)

            matches = []
            for row in rs:
                matches.append({'song_id': row[0],
                                'artist_key': row[1],
                                'album_key': row[2],
                                'title_key': row[3]})

            for match in matches:
                album_similarity = difflib.SequenceMatcher(None, match['album_key'], song_obj.album_key).ratio()
                title_similarity = difflib.SequenceMatcher(None, match['title_key'], song_obj.title_key).ratio()

                if album_similarity > 0.3 or \
                   title_similarity > 0.3 or \
                   (album_similarity > 0.1 and title_similarity > 0.1):
                    return match['song_id']

        return None

#        # First try the artist, album, track combo
#        query = """
#                SELECT s.song_id
#                  FROM song s
#                 WHERE s.artist_key = ?
#                   AND s.album_key  = ?
#                   AND s.track_key  = ?
#                """
#        values = (song_obj.artist_key,
#                  song_obj.album_key,
#                  song_obj.track_key)
#        rs = self._cursor.execute(query, values)
#        song_id = rs.fetchone()
#        rs.close()
#
#        if song_id:
#            return song_id[0]
#
#        # Otherwise try just album and track
#        query = """
#                SELECT s.song_id
#                  FROM song s
#                 WHERE s.album_key  = ?
#                   AND s.track_key  = ?
#                """
#        values = (song_obj.album_key,
#                  song_obj.track_key)
#        rs = self._cursor.execute(query, values)
#        song_id = rs.fetchone()
#        rs.close()
#
#        if song_id:
#            return song_id[0]
#
#        return None

    def _song_id_in_rating(self, song_id):
        query = """
                SELECT COUNT(*)
                  FROM music_rating mr
                 WHERE mr.song_id = ?
                """
        values = (song_id,)
        rs = self._cursor.execute(query, values)
        num_rows = rs.fetchone()[0]
        rs.close()
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
        rs.close()
        return num_rows == 1

    def close(self):
        self._cursor.close()
