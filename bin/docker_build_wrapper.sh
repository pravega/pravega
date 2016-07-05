rm -rf ./temp_repo
mkdir ./temp_repo
cp -rf ~/.m2/repository/com/twitter ./temp_repo/
cp -rf ~/.m2/repository/org/apache/bookkeeper ./temp_repo/
cp -rf ~/.m2/repository/org/apache/thrift ./temp_repo/

docker build -t streaming .
