import datetime
import json
import logging
import os
import socket
import uuid
import consul

_log = logging.getLogger(__name__)


class CannotAcquireLockError(RuntimeError):
    def __init__(self, arg):
        self.args = arg


class Semaphore:
    """
    Slightly modified version of https://gist.github.com/mastermatt/b570b0dbbdd2181cd3b8d991b1bcecff
    """

    def __init__(self, service_name, client, ttl=420, concurrency_limit=1):
        self.service_name = service_name
        self.client: consul.Consul = client
        self.ttl = ttl
        self.uuid = uuid.uuid4().hex
        self.concurrency_limit = concurrency_limit
        self.local_node_name = self.client.agent.self()['Member']['Name']
        self.prefix = 'service/{}/lock/'.format(service_name)
        self.session_id = None
        self._acquired = False

    @property
    def acquired(self):
        return self._acquired

    def acquire(self, block=True):
        if self._acquired:
            raise RuntimeError("Already acquired.")

        # Create a session and add a contender entry.
        self._clear_session()
        pid_health_check_id = self._ttl_health_check()
        self.session_id = self.client.session.create(
            name='semaphore',
            checks=['serfHealth', pid_health_check_id],
            behavior='delete',  # allows the contender entry to auto delete
            ttl=self.ttl,  # this ttl is a last resort, hence its length
        )
        _log.debug("Created session: %s.", self.session_id)

        contender_key = '{}{}'.format(self.prefix, self.session_id)
        contender_value = json.dumps({
            # The contents of this dict are opaque to Consul and
            # not used for the locking mechanism at all.
            # It's provided in the hope that it may help humans.
            'time': datetime.datetime.utcnow().isoformat(),
            'host': socket.gethostname(),
            'pid': os.getpid(),
        })
        if not self.client.kv.put(contender_key, contender_value, acquire=self.session_id):
            raise RuntimeError("Failed to add contender entry.")
        _log.debug("Created contender entry.")

        # loop until we successfully get our session saved into a slot.
        prefix_index = 0
        lock_key = '{}.lock'.format(self.prefix)

        while True:
            # The first time around the index is set to zero which
            # means it will return right away (non-blocking).
            # In the case we loop around because PUTing the new lock
            # info failed, it can be assumed the index of the prefix
            # has changed so the next call to kv.get will not block either.
            # In the case that we loop around and try again because all
            # the slots are full, this call will block until the
            # index on the prefix hierarchy is modified by another process.
            _log.debug("Getting state of semaphore. Index: %s.", prefix_index)
            prefix_index, pv = self.client.kv.get(self.prefix, index=prefix_index, recurse=True)
            _log.debug("Current index of semaphore: %s.", prefix_index)

            lock_modify_index = 0
            current_lock_data = {}
            active_sessions = set()

            for entry_value in pv:
                if entry_value['Key'] == lock_key:
                    lock_modify_index = entry_value['ModifyIndex']
                    current_lock_data = json.loads(entry_value['Value'])
                    continue

                if entry_value.get('Session'):
                    active_sessions.add(entry_value['Session'])
                    continue

                # Delete any contender entries without an active session.
                _log.debug("Deleting dead contender: %s.", entry_value['Key'])
                self.client.kv.delete(entry_value['Key'])

            sessions_in_slots = set(current_lock_data.get('holders', []))
            limit = int(current_lock_data.get('limit', self.concurrency_limit))

            if limit != self.concurrency_limit:
                _log.debug("Limit mismatch. Remote %s, Local %s.", limit, self.concurrency_limit)
                # Take the provided limit as the truth as a config may
                # have changed since the last process updated the lock.
                limit = self.concurrency_limit

            valid_sessions = active_sessions & sessions_in_slots

            if len(valid_sessions) >= limit:
                _log.debug("All slots are full. %s of %s.", len(valid_sessions), limit)
                if not block:
                    raise CannotAcquireLockError("Unable to acquire lock without blocking.")
                continue  # try again

            # Add our session to the slots and try to update the lock.
            valid_sessions.add(self.session_id)
            new_lock_value = json.dumps({
                'limit': limit,
                'holders': list(valid_sessions),
            })

            # This will not update and will return `False` if the index is out of date.
            self._acquired = self.client.kv.put(lock_key, new_lock_value, cas=lock_modify_index)
            _log.debug("Lock update attempt %s.", 'succeeded' if self._acquired else 'failed')
            if self._acquired:
                break  # yay

    def release(self):
        if not self._acquired:
            raise RuntimeError("Not acquired.")
        self._clear_session()
        self._acquired = False

    def _clear_session(self):
        if self.session_id:
            # Docs recommend updating the .lock to remove session from slot.
            # Seems like an unneeded round-trip. going to skip for now.
            _log.debug("Removing health check and session.")
            self.client.session.destroy(self.session_id)
            self.client.agent.check.deregister(self.uuid)
            self.session_id = None

    def _ttl_health_check(self):
        """
        Ensure a registered TTL check exist for this object instance.

        Does not create the check if it already exists and blocks until
        the check is passing.

        During `_clear_session` the health check is manually destroyed.
        If the process can't clean up then the check sticks around on
        the node in a critical state for 72 hours (configurable on the
        cluster) before being garbage collected.

        Note: the use of `client.health.check` is used here instead
        of `client.agent.checks` because it's possible for the local
        node to show a 'passing' status on the new health check before
        the servers in the cluster, and in order for a session to be
        created the servers must agree that the new check is 'passing'.

        :return str: the ID of the check
        """
        check_id = self.uuid
        ttl = self.ttl
        index, checks = self.client.health.node(self.local_node_name)

        if not any(check['CheckID'] == check_id for check in checks):
            _log.debug("Registering TTL health check %s.", check_id)
            registered = self.client.agent.check.register(
                name="Semaphore UUID",
                check_id=check_id,
                check={
                    'notes': 'Environment resource semaphore check status',
                    'ttl': str(self.ttl) + 's',
                },
            )
            if not registered:
                raise RuntimeError("Unable to register health check.")
            self.client.agent.check.ttl_pass(check_id)
        return check_id

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        del exc_type, exc_val, exc_tb
        try:
            self.release()
        except RuntimeError:
            pass
