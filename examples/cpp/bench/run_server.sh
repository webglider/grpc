y=$((NUM_CORES-1))
service irqbalance stop
set_irq_affinity_cpulist.sh 0-$y eno1
show_irq_affinity.sh eno1
GRPC_VERBOSITY=info GRPC_CORES_OVERRIDE=$NUM_CORES perf record -o $OP_FILE -F 999 -C 0-$y -g -- taskset -c 0-$y ./server -host 128.84.155.114 "$@"