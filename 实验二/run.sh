#!/bin/bash

set -e

CONTAINER_NAME="${CONTAINER_NAME:-h01}"
LOCAL_PROJECT_DIR="/Users/junpingchen/Documents/金融大数据处理技术/FBDP/实验二"
PY_DIR="$LOCAL_PROJECT_DIR/src"
HDFS_INPUT_DIR="/usr/local/hadoop/input"
HDFS_OUTPUT_BASE="/usr/local/hadoop/output/exp2"
HADOOP_CONF_DIR="/usr/local/hadoop/etc/hadoop"

OFFLINE_FILE="ccf_offline_stage1_train.csv"
ONLINE_FILE="ccf_online_stage1_train.csv"

MODE="${1:-all}"  # 可选：task1_offline | task1_online | task2 | task3 | task4 | all
SCRIPT_LIST=(
	task1_mapper.py
	task1_reducer.py
	task2_mapper.py
	task2_reducer.py
	task3_stage1_mapper.py
	task3_stage1_reducer.py
	task3_stage2_mapper.py
	task3_stage2_reducer.py
	task4_mapper.py
	task4_reducer.py
)

echo "====================================="
echo "实验二（Python Hadoop Streaming）运行脚本"
echo "容器: $CONTAINER_NAME"
echo "项目: $LOCAL_PROJECT_DIR"
echo "模式: $MODE"
echo "====================================="

ensure_container() {
	docker exec "$CONTAINER_NAME" bash -lc "echo 'container ok' >/dev/null"
}

copy_scripts() {
	echo "[1/5] 复制Python脚本到容器..."
	docker exec "$CONTAINER_NAME" bash -lc "mkdir -p /root/exp2_py"
	for f in "${SCRIPT_LIST[@]}"; do
		docker cp "$PY_DIR/$f" "$CONTAINER_NAME:/root/exp2_py/$f"
	done
	echo "Python脚本复制完成。"
}

copy_data() {
	echo "[2/5] 复制CSV到容器并放入HDFS..."
	# 拷贝到容器工作目录
	docker exec "$CONTAINER_NAME" bash -lc "mkdir -p $HDFS_INPUT_DIR"
	docker cp "$LOCAL_PROJECT_DIR/dataset/$OFFLINE_FILE" "$CONTAINER_NAME:$HDFS_INPUT_DIR/" 2>/dev/null
	docker cp "$LOCAL_PROJECT_DIR/dataset/$ONLINE_FILE" "$CONTAINER_NAME:$HDFS_INPUT_DIR/" 2>/dev/null || true

	# 放入HDFS
	docker exec "$CONTAINER_NAME" bash -lc "export HADOOP_CONF_DIR=$HADOOP_CONF_DIR; \
		/usr/local/hadoop/bin/hadoop fs -mkdir -p $HDFS_INPUT_DIR && \
		/usr/local/hadoop/bin/hadoop fs -put -f $HDFS_INPUT_DIR/$OFFLINE_FILE $HDFS_INPUT_DIR/ && \
		( test -f $HDFS_INPUT_DIR/$ONLINE_FILE && /usr/local/hadoop/bin/hadoop fs -put -f $HDFS_INPUT_DIR/$ONLINE_FILE $HDFS_INPUT_DIR/ || true )"
	echo "CSV放入HDFS完成。"
}

build_files_arg() {
	local joined=""
	for f in "${SCRIPT_LIST[@]}"; do
		if [ -z "$joined" ]; then
			joined="/root/exp2_py/$f"
		else
			joined="$joined,/root/exp2_py/$f"
		fi
	done
	echo "$joined"
}

run_streaming_job() {
	# $1: job_name  $2: input  $3: output  $4: mapper  $5: reducer  [$6: extra hadoop opts]
	local job_name="$1"
	local input="$2"
	local output="$3"
	local mapper="$4"
	local reducer="$5"
	local extra_opts="$6"
	local files_arg
	files_arg=$(build_files_arg)

	echo "[运行] $job_name"
	docker exec "$CONTAINER_NAME" bash -lc "
		set -e
		export HADOOP_CONF_DIR=$HADOOP_CONF_DIR
		HJAR=\$(ls /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar | head -n1)
		/usr/local/hadoop/bin/hadoop fs -rm -r -f $output 2>/dev/null || true
		/usr/local/hadoop/bin/hadoop jar \"\$HJAR\" \
			-D mapreduce.job.name=$job_name \
			$extra_opts \
			-files $files_arg \
			-input $input \
			-output $output \
			-mapper \"$mapper\" \
			-reducer \"$reducer\"
	"
}

fetch_output() {
	# $1: hdfs_output_dir  $2: local_file_name
	local hdfs_dir="$1"
	local local_name="$2"
	mkdir -p "$LOCAL_PROJECT_DIR/output"
	docker exec "$CONTAINER_NAME" bash -lc "export HADOOP_CONF_DIR=$HADOOP_CONF_DIR; /usr/local/hadoop/bin/hadoop fs -cat $hdfs_dir/part-* > /tmp/$local_name"
	docker cp "$CONTAINER_NAME:/tmp/$local_name" "$LOCAL_PROJECT_DIR/output/$local_name"
	echo "结果已保存到: $LOCAL_PROJECT_DIR/output/$local_name"
}

task1_offline() {
	run_streaming_job "exp2_task1_offline" \
		"$HDFS_INPUT_DIR/$OFFLINE_FILE" \
		"$HDFS_OUTPUT_BASE/task1_offline" \
		"python3 task1_mapper.py" \
		"python3 task1_reducer.py"
	fetch_output "$HDFS_OUTPUT_BASE/task1_offline" "task1_offline.out"
}

task1_online() {
	run_streaming_job "exp2_task1_online" \
		"$HDFS_INPUT_DIR/$ONLINE_FILE" \
		"$HDFS_OUTPUT_BASE/task1_online" \
		"python3 task1_mapper.py --online" \
		"python3 task1_reducer.py"
	fetch_output "$HDFS_OUTPUT_BASE/task1_online" "task1_online.out"
}

task2() {
	run_streaming_job "exp2_task2_distance" \
		"$HDFS_INPUT_DIR/$OFFLINE_FILE" \
		"$HDFS_OUTPUT_BASE/task2_distance" \
		"python3 task2_mapper.py" \
		"python3 task2_reducer.py"
	fetch_output "$HDFS_OUTPUT_BASE/task2_distance" "task2_distance.out"
}

task3() {
	# 阶段1
	run_streaming_job "exp2_task3_stage1" \
		"$HDFS_INPUT_DIR/$OFFLINE_FILE" \
		"$HDFS_OUTPUT_BASE/task3_stage1" \
		"python3 task3_stage1_mapper.py" \
		"python3 task3_stage1_reducer.py"
	# 阶段2（单reducer以全局排序）
	run_streaming_job "exp2_task3_stage2" \
		"$HDFS_OUTPUT_BASE/task3_stage1/part-*" \
		"$HDFS_OUTPUT_BASE/task3" \
		"python3 task3_stage2_mapper.py" \
		"python3 task3_stage2_reducer.py" \
		"-D mapreduce.job.reduces=1"
	fetch_output "$HDFS_OUTPUT_BASE/task3" "task3.out"
}

task4() {
	run_streaming_job "exp2_task4_discount" \
		"$HDFS_INPUT_DIR/$OFFLINE_FILE" \
		"$HDFS_OUTPUT_BASE/task4_discount" \
		"python3 task4_mapper.py" \
		"python3 task4_reducer.py"
	fetch_output "$HDFS_OUTPUT_BASE/task4_discount" "task4_discount.out"
}

usage() {
	echo "用法: $0 [task1_offline|task1_online|task2|task3|task4|all]"
	exit 1
}

echo ""
echo "[准备] 检查容器..."
ensure_container

copy_scripts
copy_data

case "$MODE" in
	task1_offline) task1_offline ;;
	task1_online)  task1_online  ;;
	task2)         task2         ;;
	task3)         task3         ;;
	task4)         task4         ;;
	all)
		task1_offline
		task1_online
		task2
		task3
		task4
		;;
	*) usage ;;
esac

echo ""
echo "====================================="
echo "执行完成。结果文件可在: $LOCAL_PROJECT_DIR/output"
echo "示例查看："
echo "  head -20 $LOCAL_PROJECT_DIR/output/task1_offline.out"
echo "  head -20 $LOCAL_PROJECT_DIR/output/task1_online.out"
echo "  head -20 $LOCAL_PROJECT_DIR/output/task2_distance.out"
echo "  head -20 $LOCAL_PROJECT_DIR/output/task3.out"
echo "  head -20 $LOCAL_PROJECT_DIR/output/task4_discount.out"
echo "====================================="


