#!/usr/bin/env python

from datetime import datetime


class PathBuilder:

    __slots__ = ['_bucket',
                 '_env',
                 '_source',
                 '_sub_source',
                 '_ds',
                 '_kind',
                 '_file', ]

    def __init__(self):
        self._bucket = None
        self._env = None
        self._source = None
        self._sub_source = None
        self._ds = None
        self._kind = None
        self._file = None

    def blob(self):
        assembly = self._blob_parts_list()
        return '/'.join(list(part for part in assembly if part))

    def _blob_parts_list(self):
        return [self._env,
                self._source,
                self._sub_source,
                self._ds,
                self._kind,
                self._file]

    def bucket(self, name: str, cloud: str):
        if cloud not in {'aws', }: raise Exception('Cloud not supported.')
        if name.find(':') > -1: raise Exception('Not needed!')
        prfx = dict(aws='s3',)
        self._bucket = f'{prfx[cloud]}://{name}'
        return self

    def full_path(self):
        return '/'.join(
            [self._bucket, self.blob()]
        )

    def env(self, env):
        if env not in {'dv', }:
            raise Exception('Not a legit environment!')
        self._env = f'env={env}'
        return self

    def source(self, source):
        self._source = f'source={source}'
        return self

    def subsource(self, sub_source):
        self._sub_source = f'subsrc={sub_source}'
        return self

    def kind(self, kind):
        if kind not in {'raw', }:
            raise Exception('Not an acceptable kind!')
        self._kind = f'kind={kind}'
        return self

    def ds(self, ds: str):
        """ Enter ISO 8601 compliant datestamp!!! """
        try:
            datetime.fromisoformat(ds)
        except ValueError:
            raise Exception(
                'Not a valid ISO 8601 datestamp!'
            )
        yr, mo, dy = _ds
        self._ds = f'yr={yr}/mo={mo}/dy={dy}'
        return self

    def file_name(self, name: str):
        self._file = name
        return self
