#!/usr/bin/env python3
"""Lifecycle boundary verification with real ffmpeg processes and concurrent media.

Exercises the shared media.Process lifecycle through the production HTTP path:
  * parallel renders do not kill each other (process-group isolation), including
    a render queued behind the two workers
  * render cancellation is the "cancelled" state while an aborted upload is a
    request interruption with no domain source registered — distinct states
  * the short probe timeout still fires while a normal encode keeps its own
    longer policy, and timed-out children leave no orphan processes
  * a cancelled attempt cannot late-commit an artifact
No new scheduling capability is introduced; this only drives existing routes.
"""
import json
import os
import signal
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def need(value, message):
    if not value:
        raise RuntimeError(message)


class SlowStream:
    """Reader that drips bytes so the server-side upload can be interrupted
    before the body (and therefore the probe stage) ever completes."""

    def __init__(self, payload, chunk=4096, delay=0.2):
        self.payload = payload
        self.chunk = chunk
        self.delay = delay
        self.sent = 0

    def read(self, n=-1):
        if self.sent >= len(self.payload):
            return b''
        time.sleep(self.delay)
        part = self.payload[self.sent:self.sent + self.chunk]
        self.sent += len(part)
        return part


def main():
    observations = []
    with tempfile.TemporaryDirectory(prefix='cadence-lifecycle-') as scratch:
        scratch = Path(scratch)
        ffmpeg = os.environ.get('FFMPEG_BIN', 'ffmpeg')
        ffprobe = os.environ.get('FFPROBE_BIN', 'ffprobe')
        subprocess.run([ffmpeg, '-nostdin', '-hide_banner', '-loglevel', 'error',
                        '-f', 'lavfi', '-i', 'testsrc2=size=160x90:rate=24:duration=4',
                        '-f', 'lavfi', '-i', 'sine=frequency=660:sample_rate=48000:duration=4',
                        '-c:v', 'libx264', '-preset', 'ultrafast', '-threads', '1',
                        '-c:a', 'aac', '-shortest', str(scratch / 'calibration.mp4')],
                       check=True, timeout=30)

        # Wrapper records its PID then sleeps in-place (exec keeps the PID), so
        # the probe's 15s deadline and process-group reaping are observable.
        trace = scratch / 'probe-trace.log'
        slow_wrapper = scratch / 'slow-ffprobe'
        slow_wrapper.write_text(
            '#!/bin/sh\n'
            f'echo "$$" >> {trace}\n'
            'exec sleep 20\n')
        slow_wrapper.chmod(0o755)

        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            port = sock.getsockname()[1]
        address = 'http://127.0.0.1:' + str(port) + '/forge/v1'
        base_env = dict(os.environ, FORGE_ROOT=str(scratch / 'data'),
                        FORGE_ADDR='127.0.0.1:' + str(port), FORGE_WORKERS='2')
        log = open(scratch / 'server.log', 'ab')
        process = None

        def request(method, route, body=None, headers=None, status=200, raw=False, timeout=60):
            hs = dict(headers or {})
            if isinstance(body, (dict, list)):
                body = json.dumps(body).encode()
                hs['Content-Type'] = 'application/json'
            req = urllib.request.Request(address + route, data=body, headers=hs, method=method)
            try:
                response = urllib.request.urlopen(req, timeout=timeout)
            except urllib.error.HTTPError as exc:
                response = exc
            with response:
                data = response.read()
                need(response.status == status,
                    f'{method} {route}: {response.status}, wanted {status}: {data[:400]!r}')
                return data if raw or not data else json.loads(data)

        def start(probe_binary=ffprobe):
            nonlocal process
            run_env = dict(base_env, FFPROBE_BIN=probe_binary)
            process = subprocess.Popen([str(ROOT / 'bin/forged')], cwd=ROOT, env=run_env,
                                       stdout=log, stderr=log, start_new_session=True)
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                try:
                    request('GET', '/health')
                    return
                except (OSError, RuntimeError):
                    if process.poll() is not None:
                        raise RuntimeError((scratch / 'server.log').read_text())
                    time.sleep(.05)
            raise RuntimeError('server readiness timed out')

        def stop():
            nonlocal process
            if process and process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
                process.wait(timeout=15)
            process = None

        def wait_state(rid, states, deadline_s=45):
            deadline = time.monotonic() + deadline_s
            while time.monotonic() < deadline:
                r = request('GET', '/renders/' + rid)
                if r['state'] in states:
                    return r
                time.sleep(.02)
            raise RuntimeError(f'render {rid} never reached {states}')

        def observe(name, detail):
            observations.append({'workflow_id': name, 'passed': True, 'observed': detail})

        try:
            start()
            media = (scratch / 'calibration.mp4').read_bytes()
            sid = request('POST', '/sources?name=calibration', media, status=201)['id']
            recipe = request('POST', '/recipes',
                             dict(name='r', format='mp4', width=160, height=90, fps=24,
                                  sample_rate=48000, normalize=False, target_lufs=-16),
                             status=201)
            sheet = request('POST', '/sheets', dict(
                name='s', spans=[dict(source_id=sid, in_ms=0, out_ms=4000, gain_db=0)], cues=[]),
                status=201)
            req = dict(sheet_id=sheet['id'], sheet_revision=1,
                       recipe_id=recipe['id'], recipe_revision=1)

            # 1) Concurrent media: two renders occupy the workers, a third sits
            #    queued. Cancel one running render; the others must survive.
            long_sheet = request('POST', '/sheets',
                                  dict(name='long', spans=sheet['spans'] * 16, cues=[]),
                                  status=201)
            long_req = dict(req, sheet_id=long_sheet['id'])
            submitted = []
            for i in range(3):
                r = request('POST', '/renders', long_req,
                            {'Idempotency-Key': f'concurrent-{i}'}, status=202)
                submitted.append(r['id'])
            cancelled_id, survivor_a, survivor_b = submitted
            wait_state(cancelled_id, ('running',))
            wait_state(survivor_a, ('running',))
            request('POST', f'/renders/{cancelled_id}/cancel')
            cancelled = wait_state(cancelled_id, ('cancelled',))
            need(cancelled['attempts'][-1]['outcome'] == 'cancelled',
                'cancelled attempt outcome missing')
            request('GET', f'/renders/{cancelled_id}/artifact', status=409)
            # Freed worker picks up the queued render; neither survivor is killed.
            done_a = wait_state(survivor_a, ('succeeded', 'failed'))
            need(done_a['state'] == 'succeeded',
                 f'concurrent render wrongly killed by another cancellation: {done_a["state"]}')
            done_b = wait_state(survivor_b, ('succeeded', 'failed'))
            need(done_b['state'] == 'succeeded',
                 f'queued render never ran after cancellation freed a worker: {done_b["state"]}')
            # The cancelled attempt must never late-commit, even after settling.
            time.sleep(.3)
            again = request('GET', f'/renders/{cancelled_id}')
            need(again['state'] == 'cancelled' and again.get('artifact') is None,
                'stale cancelled attempt late-committed an artifact')
            observe('concurrent-isolation',
                    'cancelled one of three renders; both others succeeded; no late commit')

            # 2) Distinct state: aborting an upload mid-transfer interrupts the
            #    request without creating a source or changing any render state.
            before = request('GET', '/state')
            count_before = len(before['sources'])
            renders_before = {rid: r['state'] for rid, r in before['renders'].items()}
            upload_req = urllib.request.Request(
                address + '/sources?name=aborted', data=SlowStream(b'x' + media), method='POST')
            try:
                resp = urllib.request.urlopen(upload_req, timeout=5)
                resp.read()
                raise RuntimeError('expected the slow upload to be interrupted')
            except (TimeoutError, urllib.error.URLError, OSError):
                pass
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                if len(request('GET', '/state')['sources']) == count_before:
                    break
                time.sleep(.05)
            state = request('GET', '/state')
            need(len(state['sources']) == count_before,
                'interrupted upload leaked a domain source')
            renders_after = {rid: r['state'] for rid, r in state['renders'].items()}
            need(renders_after == renders_before,
                'upload interruption mutated render states; it is not a render cancellation')
            observe('interruption-vs-cancellation',
                    'aborted upload registered no source and left render states untouched')

            # 3) Short probe deadline retained end-to-end (~15s) and reaped.
            stop()
            start(probe_binary=str(slow_wrapper))
            probe_start = time.monotonic()
            request('POST', '/sources?name=slow', media, status=400)
            elapsed = time.monotonic() - probe_start
            need(12 < elapsed < 19, f'probe timeout policy drifted: {elapsed:.1f}s (want ~15s)')
            lines = trace.read_text().splitlines()
            need(len(lines) == 1, f'slow probe invoked {len(lines)} times, want 1')
            wrapped_pid = int(lines[0])
            need(_is_dead(wrapped_pid), 'timed-out probe process was not reaped')
            observe('probe-timeout',
                    f'15s probe deadline retained ({elapsed:.1f}s); process group reaped')

            # 4) The formal encode keeps its own longer policy: restore the real
            #    probe (the encode's inspection stage uses it under the separate
            #    15s probe policy) and complete a render through the lifecycle.
            stop()
            start()
            normal = request('POST', '/renders', req,
                             {'Idempotency-Key': 'long-policy'}, status=202)
            done = wait_state(normal['id'], ('succeeded', 'failed'))
            need(done['state'] == 'succeeded',
                 f'legitimate encode interrupted by wrong timeout: {done["state"]}')
            observe('encode-policy', 'formal encode completed under its own deadline policy')

            # 5) No orphan media processes remain on the instance. Only PIDs
            #    the lifecycle actually launched count; ambient sleeps owned by
            #    the container's own init are out of scope.
            launched = set()
            if trace.exists():
                launched = {int(line) for line in trace.read_text().splitlines()}
            orphans = [(pid, comm) for pid, comm in _leftover_media()
                       if int(pid) in launched or comm in ('ffmpeg', 'ffprobe')]
            need(not orphans, f'orphaned media processes remain: {orphans}')
            observe('no-orphans', 'all launched media process groups reaped by the shared lifecycle')

            print(json.dumps({'ok': True, 'observations': observations}, ensure_ascii=False))
        except Exception:
            log.flush()
            print((scratch / 'server.log').read_text(errors='replace'))
            raise
        finally:
            stop()
            log.close()


def _is_dead(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    except PermissionError:
        return False
    return False


def _leftover_media():
    out = []
    for entry in Path('/proc').iterdir():
        if not entry.name.isdigit():
            continue
        try:
            comm = (entry / 'comm').read_text().strip()
        except OSError:
            continue
        if comm in ('ffmpeg', 'ffprobe', 'sleep') or 'slow-ffprobe' in comm:
            out.append((entry.name, comm))
    return out


if __name__ == '__main__':
    main()
