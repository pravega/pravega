rm -rf ./temp_repo
mkdir ./temp_repo
mkdir ./temp_repo/twitter
mkdir ./temp_repo/bookkeeper
mkdir ./temp_repo/thrift

cp -rf /root/.gradle/caches/modules-2/files-2.1/com.twitter* ./temp_repo/twitter/
cp -rf /root/.gradle/caches/modules-2/files-2.1/org.apache.bookkeeper*  ./temp_repo/bookkeeper/
cp -rf  /root/.gradle/caches/modules-2/files-2.1/org.apache.thrift*   ./temp_repo/thrift/

docker build -t streaming  -f Dockerfile_Streaming .









