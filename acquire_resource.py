#!/usr/bin/env python3

import argparse
import pickle
import semaphore
import logging
import os
import consul
import time

log = logging.getLogger(__name__)


class Resource:
    """
    A class to work with locks on some resource in a pool (e.g. to get exclusive lock on a testing environment) with
    the help of Consul.

    Each resource instance should be a member of a Consul service, which can be queried from the catalog. For example:
    $ curl http://127.0.0.1:8500/v1/catalog/services
        {
          "consul": [],
          "redis": [],
          "postgresql": [
            "instance1",
            "instance2"
          ]
        }
    here we have postgresql service and two resources - instance1 and instance2.

    To obtain a lock to one of those two:
        resource = Resource('postgres')
        resource.lock()
        ...
        <your code>
        resource.unlock()
    """
    _log = logging.getLogger(__name__)

    def __init__(self, service, ttl=420, host='127.0.0.1', port=8500, scheme='http', limit=1):
        self._log.info("Connecting to Consul")
        self.client = consul.Consul(host=host, port=port, scheme=scheme)
        self.service = service
        self.ttl = ttl
        self.timeout = 86400
        self.limit = limit
        self.semaphore = None

    def _get_resources(self):
        """
        Get list of resources in a consul service

        :return: list of resources from the service
        """
        services = self.client.catalog.services()[0]
        if self.service in services:
            return services[self.service]
        else:
            raise RuntimeError("{} serivce is not found in Consul catalog".format(self.service))

    def lock(self):
        """
        Obtain a lock to a single instance of a service.
        """
        self._log.info(f'Attempting to acquire a lock on {self.service}')
        semaphores = self._create_semaphores()
        start = time.time()
        for sem in semaphores:
            try:
                self.semaphore.acquire(block=False)
                self._log.info('Lock acquired')
                self.semaphore = sem
                break
            except semaphore.CannotAcquireLockError:
                self._log.debug(f'Cannot acquire {sem.prefix}. Trying next service.')
            if time.time() - start > self.timeout:
                raise TimeoutError('Resource acquisition timed out.')

    def _create_semaphores(self):
        """
        Create a list of Semaphore instances from a list of resources

        :return: list of Semaphore objects
        :rtype: list
        """
        semaphores = []
        for resource in self._get_resources():
            sem = semaphore.Semaphore(service_name=f"{self.service}/{resource}",
                                      ttl=self.ttl,
                                      concurrency_limit=self.limit)
            semaphores.append(sem)
        return semaphores

    def unlock(self):
        self._log.info(f'Unlocking {self.semaphore.prefix}')
        self.semaphore.release()


class ObjectToFile:
    """
    Utility class to save/load an object to/from file
    """

    @staticmethod
    def save(file_path: str, obj: object):
        """
        Saves object obj into file file_path as a binary

        :param file_path: path to the file - save destination
        :param obj: object that should be saved into the file
        """
        log.info(f'Dumping Semaphore object to {file_path}')
        with open(file_path, 'wb') as f:
            pickle.dump(obj, f)
            log.info('Dump succeeded')

    @staticmethod
    def load(file_path: str):
        """
        Loads an object from a file at file_path
        :param file_path: path to the file - load source
        :return: Object (any)
        """
        log.debug(f'Loading Semaphore object from {file_path}')
        with open(file_path, 'rb') as f:
            obj = pickle.load(f)
            log.info("Load succeeded")
        return obj


def parse_args():
    parser = argparse.ArgumentParser(description='Waits until acquired the lock on the environment resource via Consul')
    parser.add_argument('resource_name', help='name of the resource to acquire the lock')
    parser.add_argument('--host', default='127.0.0.1', help='Consul host')
    parser.add_argument('--port', default='8500', help='Consul port')
    parser.add_argument('--scheme', choices=['http', 'https'], default='http', help='Consul connection')
    parser.add_argument('--ttl', type=int, default='420', help='Lock TTL')
    parser.add_argument('--limit', type=int, default=1, help='Concurrency limit')
    parser.add_argument('--unlock', action='store_true', help='Unlock previously acquired lock')
    return parser.parse_args()


def main():
    """
    Obtain a lock on a resource, dump that lock to some file for later use and exit

    OR

    If --unlock was given, load lock info from the file and unlock the resource
    """
    args = parse_args()
    lock_file = '.lock'
    logging.basicConfig(level=logging.INFO)
    if args.unlock:
        resource: Resource = ObjectToFile.load(lock_file)
        resource.unlock()
        os.remove(lock_file)
    else:
        resource = Resource(args.resource_name, args.ttl, args.host, args.port, args.scheme, args.limit)
        resource.lock()
        ObjectToFile.save(lock_file, resource)


if __name__ == '__main__':
    main()
