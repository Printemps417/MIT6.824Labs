# 获取当前时间作为日志文件名的一部分
time=$(date +"%Y%m%d%H%M%S")

# 创建日志文件夹
mkdir -p logs

# 定义日志文件路径
log_file="logs/log$time.txt"
error_file="logs/error$time.txt"
Times=10
# 循环执行测试
for ((i=1; i<=Times; i++))
do
    echo "Running test $i..."

    # 执行测试并将结果追加到日志文件
    go test -v >> "$log_file" 2>&1

    # 检查测试结果
    if [ $? -ne 0 ]; then
        # 将错误信息写入error.txt文件
        echo "Test $i failed, check $log_file for details."
        echo "Test $i failed:" >> "$error_file"
        tail -n 500 "$log_file" >> "$error_file" # 写入最后500行日志作为错误信息
        echo "" >> "$error_file" # 添加空行以分隔不同的错误信息
    fi
done

echo "Tests completed. Logs written to $log_file."