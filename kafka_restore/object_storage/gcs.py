# Copyright (c) 2020 Aiven, Helsinki, Finland. https://aiven.io/

from .base import ObjectStorageProvider, StoredObject
from collections import namedtuple
from contextlib import contextmanager
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from http.client import IncompleteRead
from io import FileIO
from oauth2client.client import GoogleCredentials
from oauth2client.service_account import ServiceAccountCredentials

import errno
import httplib2
import json
import logging
import os
import random
import socket
import ssl
import time


DOWNLOAD_CHUNK_SIZE = 1024 * 1024 * 50
KEY_TYPE_OBJECT = "object"
KEY_TYPE_PREFIX = "prefix"

IterKeyItem = namedtuple("IterKeyItem", ["type", "value"])


# Silence Google API client verbose spamming
logging.getLogger("googleapiclient.discovery_cache").setLevel(logging.ERROR)
logging.getLogger("googleapiclient").setLevel(logging.WARNING)
logging.getLogger("oauth2client").setLevel(logging.WARNING)


class GCSProvider(ObjectStorageProvider):
    def __init__(self, *, config):
        super().__init__(config=config)
        credentials_file = self.config.get("credentials_file")
        if credentials_file is None:
            self.google_creds = GoogleCredentials.get_application_default()
        else:
            with open(credentials_file) as fh:
                credentials = json.load(fh)
            credentials_type = credentials.get("type")
            if credentials_type == "service_account":
                self.google_creds = ServiceAccountCredentials.from_json_keyfile_dict(credentials)
            elif credentials_type == "authorized_user":
                self.google_creds = ServiceAccountCredentials.from_json_keyfile_dict(credentials)
            else:
                raise ValueError(f"Unknown credentials type {credentials_type}")
        self.bucket_name = self.config["bucket"]
        self.prefix = self.config.get("prefix")
        if self.prefix and self.prefix[-1] == "/":
            self.prefix = self.prefix[:-1]
        self.gs = self._init_google_client()
        self.gs_object_client = None

    def _init_google_client(self):
        start_time = time.monotonic()
        delay = 2
        while True:
            try:
                # sometimes fails: httplib2.ServerNotFoundError: Unable to find the server at www.googleapis.com
                return build("storage", "v1", credentials=self.google_creds)
            except (httplib2.ServerNotFoundError, socket.timeout):
                if time.monotonic() - start_time > 600:
                    raise

            # retry on DNS issues
            time.sleep(delay)
            delay = delay * 2

    @contextmanager
    def _object_client(self):
        """(Re-)initialize object client if required, handle 404 errors gracefully and reset the client on
        server errors.  Server errors have been shown to be caused by invalid state in the client and do not
        seem to be resolved without resetting."""
        if self.gs_object_client is None:
            if self.gs is None:
                self.gs = self._init_google_client()
            self.gs_object_client = self.gs.objects()  # pylint: disable=no-member

        try:
            yield self.gs_object_client
        except HttpError as ex:
            if ex.resp["status"] >= "500" and ex.resp["status"] <= "599":
                self.log.error("Received server error %r, resetting Google API client", ex.resp["status"])
                self.gs = None
                self.gs_object_client = None
            raise

    def _retry_on_reset(self, request, action):
        retries = 60
        retry_wait = 2
        while True:
            try:
                return action()
            except (IncompleteRead, HttpError, ssl.SSLEOFError, socket.timeout, OSError, socket.gaierror) as ex:
                # Note that socket.timeout and ssl.SSLEOFError inherit from OSError
                # and the order of handling the errors here needs to be correct
                if not retries:
                    raise
                elif isinstance(ex, (IncompleteRead, socket.timeout, ssl.SSLEOFError, BrokenPipeError)):
                    pass  # just retry with the same sleep amount
                elif isinstance(ex, HttpError):
                    # https://cloud.google.com/storage/docs/json_api/v1/status-codes
                    # https://cloud.google.com/storage/docs/exponential-backoff
                    if ex.resp["status"] not in ("429", "500", "502", "503", "504"):  # pylint: disable=no-member
                        raise
                    retry_wait = min(10.0, max(1.0, retry_wait * 2) + random.random())
                # httplib2 commonly fails with Bad File Descriptor and Connection Reset
                elif isinstance(ex, OSError) and ex.errno not in [errno.EAGAIN, errno.EBADF, errno.ECONNRESET]:
                    raise
                # getaddrinfo sometimes fails with "Name or service not known"
                elif isinstance(ex, socket.gaierror) and ex.errno != socket.EAI_NONAME:
                    raise

                self.log.warning("%s failed: %s (%s), retrying in %.2fs",
                                 action, ex.__class__.__name__, ex, retry_wait)

            # we want to reset the http connection state in case of error
            if request and hasattr(request, "http"):
                request.http.connections.clear()  # reset connection cache

            retries -= 1
            time.sleep(retry_wait)

    def _unpaginate(self, domain, initial_op, *, on_properties):
        """Iterate thru the request pages until all items have been processed"""
        request = initial_op(domain)
        while request is not None:
            result = self._retry_on_reset(request, request.execute)
            for on_property in on_properties:
                items = result.get(on_property)
                if items is not None:
                    yield on_property, items
            request = domain.list_next(request, result)

    def _iter_keys(self, *, path=None):
        if not path:
            path = ""

        with self._object_client() as clob:
            def initial_op(domain):
                return domain.list(bucket=self.bucket_name, prefix=path, delimiter="/")

            for property_name, items in self._unpaginate(clob, initial_op, on_properties=["items", "prefixes"]):
                if property_name == "items":
                    for item in items:
                        if item["name"].endswith("/"):
                            self.log.warning("list_iter: directory entry %r", item)
                            continue  # skip directory level objects

                        yield IterKeyItem(
                            type=KEY_TYPE_OBJECT,
                            value={
                                "name": item["name"],
                                "size": int(item["size"]),
                                "last_modified": self.parse_timestamp(item["updated"]),
                                "metadata": item.get("metadata", {}),
                                "md5": self.base64_to_hex(item["md5Hash"]),
                            },
                        )
                elif property_name == "prefixes":
                    for prefix in items:
                        yield IterKeyItem(type=KEY_TYPE_PREFIX, value=prefix.rstrip("/"))
                else:
                    raise NotImplementedError(property_name)

    def list_items(self):
        for item in self._iter_keys(path=self.prefix):
            if item.type == KEY_TYPE_OBJECT:
                yield StoredObject(
                    name=item.value["name"],
                    size=item.value["size"],
                    last_modified=item.value["last_modified"],
                )

    def get_contents_to_file(self, key, filepath):
        fileobj = FileIO(filepath, mode="wb")
        done = False
        try:
            self.get_contents_to_fileobj(key, fileobj)
            done = True
        finally:
            fileobj.close()
            if not done:
                os.unlink(filepath)

    def get_contents_to_fileobj(self, key, fileobj):
        if self.prefix:
            key = f"{self.prefix}/{key}"

        self.log.debug("Starting to fetch the contents of: %r to %r", key, fileobj)
        last_log_output = 0.0
        with self._object_client() as clob:
            req = clob.get_media(bucket=self.bucket_name, object=key)
            download = MediaIoBaseDownload(fileobj, req, chunksize=DOWNLOAD_CHUNK_SIZE)
            done = False
            while not done:
                status, done = self._retry_on_reset(getattr(download, "_request", None), download.next_chunk)
                if status:
                    progress_pct = status.progress() * 100
                    now = time.monotonic()
                    if (now - last_log_output) >= 5.0:
                        self.log.debug("Download of %r: %d%%", key, progress_pct)
                        last_log_output = now
