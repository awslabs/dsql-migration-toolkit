---
name: restart-ui
description: Restart the DSQL migrator NiceGUI web UI (port 8080) and verify it
  came back up. Use when the user asks to restart the UI, reload the app, pick up
  code changes in the running UI, or says "restart ui" / "ui 재시작". Python code
  edits do NOT hot-reload (the app runs with reload=False), so the UI must be
  restarted to reflect changes to src/dsql_migrator/.
---

# Restart the DSQL migrator UI

The app is a NiceGUI server launched via `mysql-dsql-migrator ui`, bound to `127.0.0.1:8080`
(`app_host`/`app_port` from config). It runs with `reload=False`, so it must be
restarted to pick up any Python change under `src/dsql_migrator/`.

## Steps

1. Run the restart command from the repo root (kills whatever is listening on
   8080, then relaunches detached so it survives this session):

   ```bash
   PID=$(lsof -tiTCP:8080 -sTCP:LISTEN 2>/dev/null); \
   if [ -n "$PID" ]; then kill $PID 2>/dev/null; sleep 2; kill -9 $PID 2>/dev/null; fi; \
   nohup .venv/bin/python -m dsql_migrator.cli.main ui > /tmp/dsql_ui.log 2>&1 & \
   echo "relaunched pid $!"
   ```

   Run it in the background (it's a long-running server). Give it ~8 seconds to boot.

2. Verify it came back up:

   ```bash
   curl -s -o /dev/null -w "UI HTTP %{http_code}\n" --max-time 5 http://127.0.0.1:8080/
   tail -n 2 /tmp/dsql_ui.log
   ```

   Expect `UI HTTP 200` and a `NiceGUI ready to go on http://127.0.0.1:8080` log line.
   (`/sw.js not found` in the log is normal and harmless.)

3. If a CDC test insert loop is running, confirm it survived the restart (it is an
   independent OS process and should be unaffected):

   ```bash
   pgrep -f "scripts/cdc_demo_load.py" >/dev/null && echo "insert loop ALIVE" || echo "insert loop not running"
   ```

## Report

Tell the user the UI is up on http://127.0.0.1:8080 (with the HTTP code), and that
they should refresh their browser tab to see the changes. If `HTTP 200` did not come
back, show the last ~15 lines of `/tmp/dsql_ui.log` to diagnose the boot failure.

## Notes

- The relaunch uses `nohup ... &`, so the harness's background-task notification may
  report "completed" while the actual Python server keeps running — verify with the
  `curl`/`lsof` check, not the task status.
- Do NOT restart if the user explicitly asked you not to (some changes are batched
  for a later single restart).
