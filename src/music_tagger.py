#!/usr/bin/python3.1
from optparse import OptionParser
import logging
import os
import sqlite3
from src.music_tagger_db_handler import MusicTaggerDBHandler
import song as song_entity
from music_map_exceptions import UnparseableSongError


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
        self._db_handler = MusicTaggerDBHandler(self._cursor)
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
        # TODO: !3 Error handling of unparseable int
        self._rating = int(options.rating)
        # TODO: !3 Error handling of non-csv tags
        self._tags = options.csv_tags.split(',')
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
    def _tag_music(self):
        num_songs = len(self._song_set)
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

            self._db_handler.insert_rating_and_tags(song_obj, self._rating, self._tags)
        self._db_handler.close()


def main():
    MusicTagger()


if __name__ == "__main__":
    main()
