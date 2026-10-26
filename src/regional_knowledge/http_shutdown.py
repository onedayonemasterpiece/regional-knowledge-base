"""Finish accepted HTTP calls while their outbound-worker WebSocket is alive."""
import asyncio
import logging

import uvicorn

_LOG = logging.getLogger('uvicorn.error')


class HttpDrainingServer(uvicorn.Server):
    async def shutdown(self, sockets=None):
        # Freeze HTTP admission first, retaining the worker WebSocket needed by
        # accepted requests. Uvicorn normally shuts both protocols down together.
        for listener in self.servers:
            listener.close()
        for sock in sockets or []:
            sock.close()
        cycles = []
        for connection in list(self.server_state.connections):
            if hasattr(connection, 'cycle'):
                cycle = connection.cycle
                connection.shutdown()  # Disables keepalive without aborting a response.
                if cycle is not None and not cycle.response_complete:
                    cycles.append(cycle)

        timeout = self.config.timeout_graceful_shutdown
        started = asyncio.get_running_loop().time()
        try:
            if cycles and not self.force_exit:
                _LOG.info('api_http_drain state=waiting count=%s', len(cycles))
                try:
                    async with asyncio.timeout(timeout):
                        while not self.force_exit and any(
                            not c.response_complete and not c.disconnected for c in cycles
                        ):
                            await asyncio.sleep(.01)
                except TimeoutError:
                    _LOG.warning('api_http_drain state=timeout count=%s', len(cycles))
                else:
                    _LOG.info('api_http_drain state=complete count=%s', len(cycles))
            # Both phases share the existing graceful-shutdown budget.
            if timeout is not None:
                elapsed = asyncio.get_running_loop().time() - started
                self.config.timeout_graceful_shutdown = max(0, timeout - elapsed)
            await super().shutdown(sockets)
        finally:
            self.config.timeout_graceful_shutdown = timeout


def run_http_server(app, **options):
    HttpDrainingServer(uvicorn.Config(app, **options)).run()
