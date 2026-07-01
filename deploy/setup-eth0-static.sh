#!/bin/bash
# setup-eth0-static.sh
# Run once on each Pi during initial setup.
# Finds the active eth0 connection, stamps 192.168.1.10/24 as a static IP,
# and deletes stale eth0 profiles so NM always activates the right one.
set -e

TARGET_IP="192.168.1.10/24"
IFACE="eth0"

# Must be run as root or via sudo
if [ "$(id -u)" -ne 0 ]; then
  echo "Re-running with sudo..."
  exec sudo bash "$0" "$@"
fi

# Find the currently-active connection profile on eth0
ACTIVE_UUID=$(nmcli -t -f UUID,DEVICE con show --active \
  | awk -F: '$2=="eth0"{print $1; exit}')

if [ -z "$ACTIVE_UUID" ]; then
  echo "ERROR: No active $IFACE connection found."
  echo "Plug in the LAN cable and run this script again."
  exit 1
fi

echo "Active $IFACE connection UUID: $ACTIVE_UUID"

# Stamp static IP (no gateway or DNS needed — wlan0 handles internet)
nmcli con mod "$ACTIVE_UUID" \
  ipv4.method manual \
  ipv4.addresses "$TARGET_IP" \
  ipv4.gateway "" \
  ipv4.dns ""

nmcli con up "$ACTIVE_UUID"
echo "Static IP $TARGET_IP applied to $IFACE."

# Remove stale eth0 profiles (those with no DEVICE assigned)
STALE=$(nmcli -t -f UUID,DEVICE,NAME con show \
  | awk -F: '$2=="--" && ($3~/eth0/ || $3~/netplan-eth/){print $1}')

if [ -n "$STALE" ]; then
  echo "$STALE" | while IFS= read -r STALE_UUID; do
    STALE_NAME=$(nmcli -t -f UUID,NAME con show | awk -F: -v u="$STALE_UUID" '$1==u{print $2}')
    echo "Removing stale profile: $STALE_UUID ($STALE_NAME)"
    nmcli con delete "$STALE_UUID"
  done
else
  echo "No stale eth0 profiles found."
fi

echo ""
echo "Done. Verify with:"
echo "  ip -4 addr show $IFACE"
echo "  ping -c 2 192.168.1.200"
echo "  nc -vz 192.168.1.200 4000"
