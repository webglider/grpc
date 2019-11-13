for x in /sys/devices/system/cpu/cpu[1-9]*/online; do
  echo 0 >"$x"
done