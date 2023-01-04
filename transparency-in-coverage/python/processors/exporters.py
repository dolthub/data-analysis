#!/usr/bin/python3

import csv
import os

from schema import SCHEMA


class AbstractExporter(object):
    def start(self):
        raise NotImplementedError()

    def write_row(self, tablename, row):
        raise NotImplementedError()

    def write_rows(self, tablename, rows):
        raise NotImplementedError()

    def finalise(self):
        raise NotImplementedError()


class CSVExporter(AbstractExporter):
    out_dir = None
    _csv_writers = dict()
    _files = dict()

    def __init__(self, out_dir):
        super().__init__()
        self.out_dir = out_dir

    def start(self):
        os.makedirs(self.out_dir, exist_ok=True)

    def _get_file_for_table(self, tablename):
        is_new_file = False
        out_f = self._files.get(tablename)
        if out_f is None:
            file_loc = os.path.join(self.out_dir, tablename + ".csv")
            is_new_file = not os.path.exists(file_loc)
            out_f = open(file_loc, "a", encoding="utf-8")
            self._files[tablename] = out_f

        return out_f, is_new_file

    def _get_csvwriter_for_table(self, tablename):
        csv_writer = self._csv_writers.get(tablename)
        if csv_writer is None:
            out_f, is_new_file = self._get_file_for_table(tablename)
            fieldnames = SCHEMA[tablename]
            csv_writer = csv.DictWriter(
                out_f, fieldnames=fieldnames, lineterminator="\n"
            )
            if is_new_file:
                csv_writer.writeheader()
            self._csv_writers[tablename] = csv_writer

        return csv_writer

    def write_row(self, tablename, row):
        csv_writer = self._get_csvwriter_for_table(tablename)
        csv_writer.writerow(row)

    def write_rows(self, tablename, rows):
        if type(rows) == list:
            for row in rows:
                self.write_row(tablename, row)
        elif type(rows) == dict:
            self.write_row(tablename, rows)

    def finalise(self):
        for out_f in self._files.values():
            out_f.close()

        self._files = None
        self._csv_writers = None


class SQLDumpExporter(AbstractExporter):
    out_file_path = None
    _out_f = None

    def __init__(self, out_file_path):
        super().__init__()
        self.out_file_path = out_file_path

    def start(self):
        self._out_f = open(self.out_file_path, "w", encoding="utf-8")

    def write_row(self, tablename, row):
        for k in list(row.keys()):
            v = row.get(k)
            if v is None:
                del row[k]
            
            if '"' in v:
                v = v.replace('"', '\\"')
                row[k] = v

        fieldnames = list(row.keys())
        cols_comma_sep = ", ".join(fieldnames)
        values = list(map(lambda f: '"' + str(row.get(f, "")) + '"', fieldnames))
        values_comma_sep = ", ".join(values)

        sql = "INSERT IGNORE INTO {} ({}) VALUES ({});\n".format(
            tablename, cols_comma_sep, values_comma_sep
        )
        self._out_f.write(sql)

    def write_rows(self, tablename, rows):
        if type(rows) == dict:
            self.write_row(tablename, rows)
            return

        # TODO: write multiple rows in a single SQL query.
        for row in rows:
            self.write_row(tablename, row)

    def finalise(self):
        self._out_f.close()
