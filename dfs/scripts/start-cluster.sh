#!/usr/bin/env bash

script_dir="$(cd "$(dirname "$0")" && pwd)"
log_dir="${script_dir}/stdout_logs"
chunk_size=5
controller_port=8989
node_port=8988
root_dir="/bigdata/bpporter/p1-file-system/"

source "${script_dir}/nodes.sh"

echo "Installing..."
go install ../controller/controller.go   || exit 1 # Exit if compile+install fails
go install ../node/node.go || exit 1 # Exit if compile+install fails
echo "Done!"

echo "Creating log directory: ${log_dir}"
mkdir -pv "${log_dir}"

echo "Starting Controller..."
ssh "${controller}" "${HOME}/go/bin/controller ${controller_port} ${chunk_size}" &> "${log_dir}/controller.log" &

echo "Starting Storage Nodes..."
for node in ${nodes[@]}; do
    echo "${node}"
    ssh "${node}" "${HOME}/go/bin/node ${node_port} ${root_dir} ${controller} ${controller_port}" &> "${log_dir}/${node}.log" &
done

echo "Startup complete!"