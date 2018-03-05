#!/usr/bin/env python3

import sys
import os
import hashlib
import json

import singer
# import singer.metrics as metrics
import singer.utils as singer_utils
# from singer import metadata

from genson import SchemaBuilder
import pandas as pd
from pygrok import Grok

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    'directories',
    'file_format'
]

CONFIG = {
    'directories': []
}

# via https://stackoverflow.com/a/1131255
def calc_md5(filename, blocksize=2**20):
    m = hashlib.md5()
    with open(filename, 'rb') as f:
        while True:
            buf = f.read(blocksize)
            if not buf:
                break
            m.update( buf )
    return m.hexdigest()

class TapText(object):
    """
    Attributes:
        directories: A list of directory names containing source files
        state: Object containing state info like previously seen file hashes and schemas
        file_format: Input file format. Either JSONL or CSV
    """

    def __init__(self, directories=[], state={}, file_format='jsonl', rec_hash_keys=False):
        self.directory_names = [d[0:-1] if d[-1] == '/' else d  for d in directories]
        self.state = state
        self.file_format = file_format
        self.rec_hash_keys = rec_hash_keys

        self.directories = {}
        self._build_file_list()

    def _build_file_list(self):
        """Create internal mapping of input file objects to potentially sync."""

        if not self.state.get('previously_seen_files'):
            self.state['previously_seen_files'] = {}

        total_file_count = 0
        for dir in self.directory_names:
            LOGGER.info('Finding {} files in {}...'.format(self.file_format, dir))

            dirname = os.path.basename(dir)
            file_count = 0

            if not self.state['previously_seen_files'].get(dirname):
                self.state['previously_seen_files'][dirname] = []

            for filename in os.listdir(dir):
                if filename.endswith('.' + self.file_format):
                    if not self.directories.get(dir):
                        self.directories[dir] = {'dirname': dirname,
                                                 'files': []}
                    absolute_path = os.path.join(dir, filename)
                    file_hash = calc_md5(absolute_path)

                    if file_hash in self.state['previously_seen_files'][dirname]:
                        LOGGER.info('File {} has not changed since last run so it will be skipped'.format(filename))
                    else:
                        self.directories[dir]['files'].append({'filename': filename,
                                                            'absolute_path': absolute_path,
                                                            'hash': file_hash})
                        self.state['previously_seen_files'][dirname].append(file_hash)
                        file_count += 1

            LOGGER.info('Found {} new or newly-changed {} files in {}'.format(file_count, self.file_format, dir))
            total_file_count += file_count

        LOGGER.info('Found {} total new or newly-changed {} files in {} directories'.format(total_file_count,
                                                                                            self.file_format,
                                                                                            len(self.directories)))

    def _add_key_to_rec(self, record, str_rep=None):
        if self.rec_hash_keys:
            if not str_rep:
                str_rep = json.dumps(record, sort_keys=True)
            record['_singer_gen_key'] = hashlib.md5(str_rep.encode('utf-8')).hexdigest()
        return record

    def build_schemas(self):
        """Do a pass over the files and use GenSon to generate their schemas"""

        # TODO add sampling so that we don't have to pass over every single record

        LOGGER.info('Building schemas')

        if not self.state.get('schemas'):
            self.state['schemas'] = {}

        for dirpath, d in self.directories.items():
            dirname = d['dirname']
            LOGGER.info('Building schema for `{}`'.format(dirname))
            schema_builder = SchemaBuilder()

            if not self.state['schemas'].get(dirname):
                self.state['schemas'][dirname] = {"type": "object", "properties": {}}
            else:
                LOGGER.info("Existing schema for `{}` will be used as seed schema".format(dirname))

            schema_builder.add_schema(self.state['schemas'][dirname])

            for f in d['files']:
                if self.file_format == 'jsonl':
                    for line in open(f['absolute_path'], 'r'):
                        parsed_line = json.loads(line)
                        parsed_line = self._add_key_to_rec(parsed_line, line)
                        schema_builder.add_object(parsed_line)
                elif self.file_format == 'csv':
                    # Note: parsing dates is pointless until date formatting support in GenSon
                    for df in pd.read_csv(f['absolute_path'], parse_dates=False, chunksize=1):
                        rec = df.to_dict('records')[0]
                        rec = self._add_key_to_rec(rec)
                        schema_builder.add_object(rec)
                elif self.file_format == 'log':
                    # TODO Use pattern per table and get it not from config
                    grok = Grok(CONFIG['grok_pattern'])
                    for line in open(f['absolute_path'], 'r'):
                        parsed_line = grok.match(line)
                        if not parsed_line:
                            parsed_line = {}
                        parsed_line['_sdc_raw_log_line'] = line
                        schema_builder.add_object(parsed_line)

            self.directories[dirpath]['schema'] = schema_builder.to_schema()
            self.state['schemas'][dirname] = self.directories[dirpath]['schema']

        LOGGER.info('Done building schemas')


    def do_sync(self):
        """Read data out of text files and write it to stdout following Singer spec"""

        LOGGER.info("Extracting data")

        # read/persist the file in batches
        RECORDS_PER_BATCH = 100

        for dirpath, d in sorted(self.directories.items()):
            dirname = d['dirname']
            key_properties = ['_singer_gen_key'] if self.rec_hash_keys else []
            LOGGER.info('Writing schema for `{}`'.format(dirname))
            singer.write_schema(dirname, d['schema'], key_properties)
            if len(d['files']) > 0:
                LOGGER.info('Extracting data from `{}`'.format(dirname))
                lines_to_write = []
                for f in sorted(d['files'], key=lambda x: x['filename']):
                    if self.file_format == 'jsonl':
                        for line in open(f['absolute_path'], 'r'):
                            parsed_line = json.loads(line)
                            parsed_line = self._add_key_to_rec(parsed_line, line)
                            lines_to_write.append(parsed_line)
                            if len(lines_to_write) >= RECORDS_PER_BATCH:
                                singer.write_records(dirname, lines_to_write)
                                lines_to_write = []
                    elif self.file_format == 'csv':
                        for df in pd.read_csv(f['absolute_path'], parse_dates=False, chunksize=1):
                            rec = df.to_dict('records')[0]
                            rec = self._add_key_to_rec(rec)
                            lines_to_write.append(rec)
                            if len(lines_to_write) >= RECORDS_PER_BATCH:
                                singer.write_records(dirname, lines_to_write)
                                lines_to_write = []
                    elif self.file_format == 'log':
                        # TODO Use pattern per table and get it not from config
                        grok = Grok(CONFIG['grok_pattern'])
                        for line in open(f['absolute_path'], 'r'):
                            parsed_line = grok.match(line)
                            if not parsed_line:
                                parsed_line = {}
                            parsed_line['_sdc_raw_log_line'] = line
                            lines_to_write.append(parsed_line)
                            if len(lines_to_write) >= RECORDS_PER_BATCH:
                                singer.write_records(dirname, lines_to_write)
                                lines_to_write = []

                    singer.write_records(d['dirname'], lines_to_write)
                    lines_to_write = []

                singer.write_records(d['dirname'], lines_to_write)
            LOGGER.info('Writing state for `{}`'.format(dirname))
            singer.write_state(self.state)

        LOGGER.info('Writing final state')
        singer.write_state(self.state)

def main_impl():
    LOGGER.info("=== Welcome to tap-text! ===")

    LOGGER.info('Reading config file')
    args = singer_utils.parse_args(REQUIRED_CONFIG_KEYS)
    CONFIG.update(args.config)
    LOGGER.info('Going to sync data from {} directories'.format(len(CONFIG['directories'])))

    rec_hash_keys = CONFIG.get('rec_hash_keys', False)
    file_format = CONFIG.get('file_format', 'jsonl')
    tap = TapText(CONFIG['directories'],
                  state=args.state,
                  file_format=file_format,
                  rec_hash_keys=rec_hash_keys)
    tap.build_schemas()
    tap.do_sync()


def main():
    try:
        main_impl()
    except Exception as e:
        LOGGER.critical(e)
        raise e
