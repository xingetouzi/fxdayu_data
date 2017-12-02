import bcolz
import pandas as pd
from fxdayu_data.data_api.basic import BasicReader


class BLPTable(BasicReader):

    def __init__(self, rootdir, index):
        self.table = bcolz.ctable(rootdir=rootdir, mode='r')
        self.line_map = self.table.attrs.attrs['line_map']
        self.index_col = index
        self.columns = list(self.table.names)
        self.columns.remove(index)

    @property
    def index(self):
        return self.table.cols[self.index_col]

    def read(self, name, fields=None, start=None, end=None, length=None):
        if fields is None:
            fields = self.columns

        return self._read(name, fields, start, end, length)

    def _read(self, name, columns, start, end, length):
        index_slice = self._index_slice(name, start, end, length)
        data = pd.DataFrame(
            {key: self._read_line(index_slice, key) for key in columns},
            index=self._read_index(index_slice)
        )
        data.index.name = self.index_col
        return data

    def _read_line(self, index, column):
        return self.table.cols[column][index]

    def _read_index(self, index):
        return self.index[index]

    def _index_slice(self, name, start, end, length):
        # head, tail = self.line_map[name]
        head, tail = self.line_map.get(name, (0, 0))
        index = self.index[head:tail]
        if start:
            s = index.searchsorted(start)
            if end:
                e = index.searchsorted(end, 'right')
                return slice(head+s, head+e)
            elif length:
                return slice(head+s, head+s+length)
            else:
                return slice(head+s, tail)
        elif end:
            e = index.searchsorted(end, 'right')
            if length:
                return slice(head+e-length, head+e)
            else:
                return slice(head, head+e)
        elif length:
            return slice(tail-length, tail)
        else:
            return slice(head, tail)

#
# class ConvertTable(BLPTable):
#
#      def __init__(self, rootdir, index, convert_input=None, convert_output=None):
#          super(ConvertTable, self).__init__(rootdir, index)
#          self.co = convert_output
#          self.ci = convert_input
#
#      def read(self, name, fields=None, start=None, end=None, length=None):
#
#          return self.handler(super(ConvertTable, self).read(name, fields, start, end, length))
