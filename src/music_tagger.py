#!/usr/bin/python3.1
from optparse import OptionParser
import logging
import os
import unicodedata
import sqlite3
from src.music_tagger_db_handler import MusicTaggerDBHandler


class MusicTagger(object):

    def __init__(self):
        self._parse_options()
        self._handle_logging(self._debug)
        self._conn = sqlite3.connect(self._db_loc)
        self._cursor = self._conn.cursor()
        self._cursor.execute('PRAGMA synchronous=OFF')
        self._cursor.execute('PRAGMA count_changes=OFF')
        self._cursor.execute('PRAGMA journal_mode=MEMORY')
        self._cursor.execute('PRAGMA temp_store=MEMORY')
        self._db_handler = MusicTaggerDBHandler(self._conn)
        self._validate()
        self._song_set = self._build_song_set()
        self._music_map = self._tag_music()
        self._conn.commit()
        self._conn.close()

    def _parse_options(self):
        parser = OptionParser()
        parser.add_option("-p", "--playlist", dest="playlist_loc",
                           help="Location of the playlist you want to make a " \
                                "map for.", metavar="PLAYLIST")
        parser.add_option('-r', "--rating", dest="rating",
                          help="The rating that you want to give all the songs " \
                               "in this playlist.", metavar="RATING")
        parser.add_option('-t', "--tags", dest="csv_tags",
                          help="A comma-delimited list of tags to give all " \
                               "the songs in the playlist.", metavar="TAGS")
        parser.add_option("--music_root", dest="music_root",
                          help="The full path to the root of the music tree " \
                               "inside the playlist.", metavar="ROOT_PATH")
        parser.add_option("-d", "--debug", action="store_true", dest="debug",
                           help="Set this flag if you want logging " \
                                "to be set to debug.", default=False)
        parser.add_option("--db", dest="db_loc", help="Location of the DB",
                          metavar="DB")

        options = parser.parse_args()[0]
        self._playlist_loc = os.path.abspath(options.playlist_loc)
        # TODO: !# Error handling of unparseable int
        self._rating = int(options.rating)
        self._tags = self._parse_tags(options.csv_tags)
        self._music_root = options.music_root
        self._db_loc = options.db_loc
        self._debug = options.debug

    # TODO: !3 Logging ini file?
    def _handle_logging(self, debug):
        self._logger = logging.getLogger("music_tagger")

        self._logger.setLevel(logging.DEBUG)

        file_handler = logging.FileHandler("music_tagger.log", mode='w')
        file_handler.setLevel(logging.DEBUG)

        console_handler = logging.StreamHandler()
        if debug:
            console_handler.setLevel(logging.DEBUG)
        else:
            console_handler.setLevel(logging.INFO)

        self._logger.addHandler(console_handler)
        self._logger.addHandler(file_handler)

        # TODO: !3 Factory for logs?
        self._unparseable = logging.getLogger("unparseable")
        self._unparseable.setLevel(logging.DEBUG)
        unparseable_handler = logging.FileHandler("unparseable.log", mode="w")
        unparseable_handler.setLevel(logging.DEBUG)
        self._unparseable.addHandler(unparseable_handler)

        self._unknown_error = logging.getLogger("unknown_error")
        self._unknown_error.setLevel(logging.DEBUG)
        unknown_error_handler = logging.FileHandler("unknown_error.log", mode="w")
        unknown_error_handler.setLevel(logging.DEBUG)
        self._unknown_error.addHandler(unknown_error_handler)

    def _validate(self):
        try:
            open(self._playlist_loc)
        except IOError as ioe:
            self._logger.exception(ioe)
            exit("The playlist you wanted to map does not exist: {0}"
                 .format(self._playlist_loc))
        self._logger.info("Using the playlist '{0}'.".format(self._playlist_loc))

    def _build_song_set(self):
        playlist = open(self._playlist_loc, 'r')
        songs = set(playlist)
        self._logger.debug("{0} of songs in playlist.".format(len(songs)))
        return songs

    # TODO: !2 Handle exceptions consistently and with appropriate logging,
    # especially for unparseable stuff.
    # TODO: !2 Threading?
    def _build_music_map(self):
        num_songs = len(self._song_set)
        cursor = self._conn.cursor()
        cursor.execute('PRAGMA synchronous=OFF')
        cursor.execute('PRAGMA count_changes=OFF')
        cursor.execute('PRAGMA journal_mode=MEMORY')
        cursor.execute('PRAGMA temp_store=MEMORY')
        for i, song in enumerate(self._song_set):
            try:
                song_obj = song_entity.Song(song)
            except UnparseableSongError:
                self._logger.debug("Error parsing info out of '{0}'. Continuing."
                                   .format(song))
                self._unparseable.error(song)
                continue
            finally:
                if i % 100 == 0 or i == num_songs - 1:
                    self._logger.info("{0}/{1}".format(i + 1, num_songs))

            self._db_handler.insert_song(cursor, song_obj, self._music_root)
        cursor.close()

    # TODO: !3 Put into a utility function somewhere.
    @staticmethod
    def sanitize_string(s, remove_the=False, remove_and=False):
        s = s.lower()
        s = s.replace("_", " ")
        s = s.replace(".", "")
        s = s.replace("-", " ")
        s = s.replace("(", "")
        s = s.replace(")", "")
        if remove_the:
            s = s.replace(" the", "")
            s = s.replace("the ", "")

        if remove_and:
            s = s.replace("& ", "")
            s = s.replace(" &", "")
            s = s.replace("and ", "")
            s = s.replace(" and", "")
        # Change special characters into their somewhat normal equivalent
        s = unicodedata.normalize('NFKD', s).encode('ascii', 'ignore').decode('utf-8')
        s = s.rstrip()
        return s

    def get_by_track(self, artist, album, track):
        ss = MusicMap.sanitize_string
        if ss(artist, remove_and=True, remove_the=True) not in self._music_map:
            raise KeyError("Unable to find artist {0}".format(artist))
        albums = self._music_map[ss(artist, remove_and=True, remove_the=True)]

        if ss(album) not in albums:
            raise KeyError("Unable to find album {0} from artist {1}"
                           .format(album, artist))
        tracks = albums[ss(album)]

        if ss(track) not in tracks:
            raise KeyError("Unable to find track {track} from album {album} " \
                           "from artist {artist}"
                           .format(track=track,
                                   album=album,
                                   artist=artist))
        return albums[ss(album)][ss(track)]

    def get_by_title(self, artist, album, title):
        ss = MusicMap.sanitize_string
        if ss(artist, remove_and=True, remove_the=True) not in self._music_map:
            raise KeyError("Unable to find artist {0}".format(artist))
        albums = self._music_map[ss(artist, remove_and=True, remove_the=True)]

        if ss(album) not in albums:
            raise KeyError("Unable to find album {0} from artist {1}"
                           .format(album, artist))
        tracks = albums[ss(album)]

        for _, existing_title in tracks.items():
            if ss(title) == existing_title:
                return existing_title
        raise KeyError("Unable to find title {title} from album {album} " \
                       "from artist {artist}"
                       .format(title=title,
                               album=album,
                               artist=artist))

    def __iter__(self):
        return self

    def __next__(self):
        self._index = self._index + 1
        if self._index >= len(self._all_tracks):
            raise StopIteration
        else:
            return self._all_tracks[self._index]


def main():
    MusicMap()


if __name__ == "__main__":
    main()
