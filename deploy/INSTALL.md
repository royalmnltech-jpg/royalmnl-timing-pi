# RoyalMNL Timing Node — Install Guide

Throughout this guide, replace `<NODE_USER>` with the OS username configured for this Pi
via Raspberry Pi Imager (e.g. `royalmnl-1`, `royalmnl-2`).

---

## 1 — Clone the repo

```bash
git clone https://github.com/royalmnl/royalmnl-timing-pi.git \
    /home/<NODE_USER>/royalmnl-timing-pi
```

Verify:

```bash
ls /home/<NODE_USER>/royalmnl-timing-pi/main.py
```

---

## 2 — Create the env file

Copy from the template and open for editing:

```bash
sudo cp /home/<NODE_USER>/royalmnl-timing-pi/deploy/royalmnl-timing-node.env.template \
        /etc/royalmnl-timing-node.env

sudo chown root:root /etc/royalmnl-timing-node.env
sudo chmod 600 /etc/royalmnl-timing-node.env

sudo nano /etc/royalmnl-timing-node.env
```

At minimum, set these four (replace all `<NODE_USER>` occurrences):

```
TIMING_NODE_ID=<NODE_USER>
TIMING_API_BASE_URL=https://royalmnl-timing-server.fly.dev
TIMING_API_KEY=<your-api-key>
TIMING_DB_PATH=/home/<NODE_USER>/.royalmnl-timing/outbox.db
```

Verify:

```bash
sudo cat /etc/royalmnl-timing-node.env
```

---

## 3 — Install the systemd unit

Replace `<NODE_USER>` in the service file with the actual username:

```bash
sed -i "s/<NODE_USER>/royalmnl-1/g" \
    /home/<NODE_USER>/royalmnl-timing-pi/deploy/royalmnl-timing-node.service
```

> **Or** open and edit manually:
> ```bash
> nano /home/<NODE_USER>/royalmnl-timing-pi/deploy/royalmnl-timing-node.service
> ```
> Replace every `<NODE_USER>` with the actual username, then save.

Copy to systemd:

```bash
sudo cp /home/<NODE_USER>/royalmnl-timing-pi/deploy/royalmnl-timing-node.service \
        /etc/systemd/system/royalmnl-timing-node.service

sudo systemctl daemon-reload
```

Verify the unit (check `User=`, `WorkingDirectory=`, `ExecStart=`):

```bash
sudo systemctl cat royalmnl-timing-node.service
```

---

## 4 — Pre-flight: manual run

Before enabling the service, run the app manually to confirm it starts:

```bash
cd /home/<NODE_USER>/royalmnl-timing-pi
export $(sudo cat /etc/royalmnl-timing-node.env | grep -v "^#" | xargs)
python3 main.py
```

Expected output on success:
```
INFO [timing-node] Connecting reader 192.168.1.200:4000
INFO [timing-node] Reader capture mode: 4-antenna inventory (0x89)
INFO [timing-node] Backend ONLINE — assigned event=te_xxx checkpoint=finish v=1
```

Press `Ctrl+C` to stop. Fix any errors before continuing.

---

## 5 — Enable and start

```bash
sudo systemctl enable royalmnl-timing-node.service
sudo systemctl start royalmnl-timing-node.service
```

---

## 6 — Verify it's running

```bash
systemctl status royalmnl-timing-node.service --no-pager
```

Expected: `Active: active (running)`.

Watch live logs to confirm backend connection:

```bash
journalctl -u royalmnl-timing-node.service -f
```

Look for:
```
INFO [timing-node] Backend ONLINE — assigned event=te_xxx checkpoint=... v=1
```

---

## 7 — Live logs

```bash
# Follow live output:
journalctl -u royalmnl-timing-node -f

# Last 100 lines:
journalctl -u royalmnl-timing-node -n 100

# Since last boot:
journalctl -u royalmnl-timing-node -b
```

---

## 8 — Stop / restart / disable

```bash
sudo systemctl stop royalmnl-timing-node       # graceful drain (up to 75s)
sudo systemctl restart royalmnl-timing-node
sudo systemctl disable royalmnl-timing-node    # remove from boot
```

---

## 9 — Update the software

```bash
cd /home/<NODE_USER>/royalmnl-timing-pi
git pull
sudo systemctl restart royalmnl-timing-node.service
```

Verify after restart:

```bash
systemctl status royalmnl-timing-node.service --no-pager
journalctl -u royalmnl-timing-node.service -n 30 --no-pager
```

---

## 10 — Adding or editing env vars

To append new variables:

```bash
sudo tee -a /etc/royalmnl-timing-node.env >/dev/null <<'EOF'
FAST_SWITCH_ENABLED=1
FAST_SWITCH_ANT_COUNT=auto
WORK_ANTENNA_QUERY=0
EOF
```

Verify no duplicates:

```bash
sudo cat /etc/royalmnl-timing-node.env
```

Apply by restarting:

```bash
sudo systemctl restart royalmnl-timing-node.service
sudo journalctl -u royalmnl-timing-node.service -n 50 --no-pager
```

For single-line edits:

```bash
sudo nano /etc/royalmnl-timing-node.env
```

---

## 11 — Migrating from an old setup

### From rc.local or cron @reboot

```bash
# Check for existing cron entry:
crontab -l | grep main.py

# Remove it:
crontab -e   # delete the @reboot line

# Check rc.local:
sudo nano /etc/rc.local   # remove any 'python3 .../main.py &' line
```

Kill any running instance before enabling the service:

```bash
sudo pkill -f main.py
```

### From `systemctl edit --force --full`

If the unit was previously created via `systemctl edit --force --full`, it already exists
at `/etc/systemd/system/royalmnl-timing-node.service`. Overwrite it with the `cp` command
from Step 3, or edit it in place:

```bash
sudo systemctl edit --force --full royalmnl-timing-node.service
```

Always run `sudo systemctl daemon-reload` after any unit file change.

---

## Race-day verification checklist

Run after first install and after any major software update:

- [ ] `systemctl enable --now` → physical reboot → node auto-starts after network + NTP
- [ ] Dashboard shows node as Online within 30s of boot
- [ ] Wave a tag at the reader → read appears in dashboard live feed
- [ ] `sudo systemctl stop royalmnl-timing-node` mid-capture → clean shutdown (no `SIGKILL` in logs), WAL checkpointed (`journal` shows "WAL checkpoint"), queued rows still in DB on next start
- [ ] `sudo kill -9 $(pgrep -f main.py)` → `Restart=on-failure` brings it back within 5s
- [ ] `journalctl -u royalmnl-timing-node` shows structured timestamps operators can read without SSH

---

## Troubleshooting

| Symptom | Check |
|---|---|
| Service fails to start | `journalctl -u royalmnl-timing-node -n 50` — look for missing env vars or bad paths |
| "TIMING_NODE_ID is required" loop | Edit `/etc/royalmnl-timing-node.env`, `sudo systemctl restart` |
| Reader not connecting | Verify `READER_IP` and `READER_PORT`, confirm reader is on same LAN |
| No assignment / DEGRADED | Check node is assigned a checkpoint in dashboard → Checkpoints & Nodes |
| 401 sync errors | `TIMING_API_KEY` mismatch; update env file and restart |
| 422 sync errors | Event not in `live` status; flip event live in dashboard |
| DB path error on start | Confirm `TIMING_DB_PATH` has `<NODE_USER>` replaced and the directory is writable |
| Unit shows wrong User= | Confirm `<NODE_USER>` was replaced in service file; run `sudo systemctl cat royalmnl-timing-node.service` |
