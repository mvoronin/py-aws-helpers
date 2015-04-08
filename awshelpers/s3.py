# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
import sys
import errno
import os
from pypbar import ProgressBar
from boto.s3.connection import S3Connection, Location
from boto.exception import S3ResponseError, S3CreateError
from boto.s3.key import Key


class AWSS3(object):
    __access_key_id = ''
    __secret_access_key = ''
    __connection = None
    __logger = None

    def __init__(self, key, secret, logger=None):
        """
        :type key: str
        :param key: AWS ACCESS KEY ID.

        :int secret: str
        :param secret: AWS SECRET ACCESS KEY.

        :type logger: logging.Logger
        :param logger: Logger object which will be used for logging.
        """

        self.__access_key_id = key
        self.__secret_access_key = secret
        self.__logger = logger
        self.connect()

    def connect(self):
        """
        :return: boto.s3.connection.S3Connection
        """

        self.__connection = S3Connection(self.__access_key_id, self.__secret_access_key)
        return self.__connection

    def create_bucket(self, bucket_name, location=Location.DEFAULT):
        """
        :type bucket_name: str
        :param bucket_name: Bucket name.

        :type location: str
        :param location: AWS S3 Location.

        :return: boto.s3.bucket.Bucket
        """

        try:
            bucket = self.__connection.create_bucket(bucket_name, location)
            self.__logger.info("Bucket \"%s\" was created!" % bucket_name)
        except S3CreateError as e:
            self.__logger.error("Can't create a new bucket! ({0}): {1}".format(e.status, e.reason))
            raise e

        return bucket

    def get_bucket(self, bucket_name):
        """
        :type bucket_name: str
        :param bucket_name: Bucket name.

        :return: boto.s3.bucket.Bucket
        """

        try:
            bucket = self.__connection.get_bucket(bucket_name)
            self.__logger.info("Connected with bucket \"%s\"!" % bucket_name)
        except S3ResponseError as e:
            self.__logger.error("Can't get a bucket \"%s\"! (%s): %s" % (bucket_name, e.status, e.reason))
            raise e

        return bucket

    def upload_file(self, bucket, str_key, path_file):
        """
        :type bucket: boto.s3.bucket.Bucket
        :param bucket: Bucket object.

        :type str_key: str
        :param str_key: AWS S3 Bucket key.

        :type path_file: str
        :param path_file: Path to a file.
        """

        self.__logger.info('Uploading "%s" to [%s]:%s' % (path_file, bucket.name, str_key))

        k = Key(bucket)
        k.key = str_key
        pb = ProgressBar(width=20, prefix='%s ' % str_key)
        size = k.set_contents_from_filename(path_file, cb=pb.update)
        pb.finish()

    def upload_file1(self, bucket_name, key, file_path):
        """
        :type bucket_name: str
        :param bucket_name: Bucket name.

        :type key: str
        :param key: AWS S3 bucket key.

        :type file_path: str
        :param file_path: Path to a file.
        """

        bucket = self.get_bucket(bucket_name)
        return self.upload_file(bucket, key, file_path)

    def upload_directory(self, bucket, dir_path, key_prefix='/'):
        """
        :type bucket: boto.s3.bucket.Bucket
        :param bucket: Bucket object.

        :type dir_path: str
        :param dir_path: Path to a directory for uploading.

        :type key_prefix: str
        :param key_prefix: S3 key prefix.
        """

        self.__logger.info('Uploading directory "%s" to [%s]:%s' % (dir_path, bucket.name, key_prefix))

        try:
            filelist = os.listdir(dir_path)
        except OSError:
            self.__logger.error("Can't read a directory \"%s\"!" % dir_path)
            sys.exit(1)

        for filename in filelist:
            k = Key(bucket)
            k.key = "%s/%s" % (os.path.basename(dir_path), filename)
            pb = ProgressBar(prefix='%s ' % k.key)
            size = k.set_contents_from_filename(os.path.join(dir_path, filename), cb=pb.update)

    def upload_directory1(self, bucket_name, dir_path):
        """
        :type bucket_name: str
        :param bucket_name: Bucket name.

        :type dir_path: str
        :param dir_path: Path to a directory for uploading.
        """

        bucket = self.get_bucket(bucket_name)
        return self.upload_directory(bucket, dir_path)

    def download_file(self, bucket, str_key, path_destination):
        """
        :type bucket: boto.s3.bucket.Bucket
        :param bucket: Bucket object.

        :type str_key: str
        :param str_key: AWS S3 Bucket key.

        :type path_destination: str
        :param path_destination: Destination directory for saving.
        """

        self.__logger.info('Downloading file "%s" from [%s] to "%s"' % (str_key, bucket.name, path_destination))

        try:
            os.makedirs(path_destination)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                self.__logger.error("Can't create a directory \"%s\"!" % path_destination)
                raise exception
        file_path = os.path.join(path_destination, str_key)
        k = Key(bucket)
        k.key = str_key
        pb = ProgressBar(prefix='%s ' % str_key)
        k.get_contents_to_filename(file_path, cb=pb.update)

    def download_file1(self, bucket_name, str_key, path_destination):
        """
        :type bucket_name: str
        :param bucket_name: Bucket name.

        :type str_key: str
        :param str_key: AWS S3 Bucket key.

        :type path_destination: str
        :param path_destination: Destination directory for saving.
        """

        bucket = self.get_bucket(bucket_name)
        return self.download_file(bucket, str_key, path_destination)

    def download_directory(self, bucket, key_prefix, path_destination):
        """
        :type bucket: boto.s3.bucket.Bucket
        :param bucket: Bucket object.

        :type key_prefix: str
        :param key_prefix: AWS S3 Bucket key prefix (this is like FS path, S3 is a key-value storage).

        :type path_destination: str
        :param path_destination: Destination directory for saving.
        """

        self.__logger.info('Downloading directory "%s" from [%s] to "%s"' % (key_prefix, bucket.name, path_destination))

        try:
            os.makedirs(path_destination)
        except OSError as exception:
            self.__logger.error("Can't create a directory \"%s\"!" % path_destination)
            if exception.errno != errno.EEXIST:
                raise exception

        keys = bucket.get_all_keys(prefix=key_prefix)
        for ind, key in enumerate(keys):
            path_file = os.path.join(path_destination, key.key)
            path_file_dir = os.path.dirname(path_file)
            if not os.path.exists(path_file_dir):
                os.makedirs(path_file_dir)
            pb = ProgressBar(prefix='...%s ' % key.key[-20:], postfix=' File %s of %s' % (ind, len(keys)))
            key.get_contents_to_filename(path_file, cb=pb.update)
            pb.finish()

    def download_directory1(self, bucket_name, key_prefix, path_destination):
        """
        :type bucket_name: str
        :param bucket_name: Bucket name.

        :type key_prefix: str
        :param key_prefix: AWS S3 Bucket key prefix (this is like FS path, S3 is a key-value storage).

        :type path_destination: str
        :param path_destination: Destination directory for saving.
        """

        bucket = self.get_bucket(bucket_name)
        return self.download_directory(bucket, key_prefix, path_destination)
