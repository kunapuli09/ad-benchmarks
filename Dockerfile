FROM golang:1.8
MAINTAINER Krishna Kunapuli <krishna_kunapuli@starpathit.com>

# setup env
ENV projectdir $GOPATH/src/github.com/kunapuli09/ad-benchmarks/
#ENV zookeepers "10.0.0.131:2181"
ENV broker "10.0.0.131:9092"
ENV events_topic "ad-events"
ENV group_id "ad-golang1"
ENV workers 5
ENV restart_interval "5m"
ENV redis_url "10.0.0.131:6379"
ENV redis_database "ads"

# install godeps
RUN go get github.com/tools/godep

# run tests & install
COPY / $projectdir
WORKDIR $projectdir
RUN godep go test ./...
RUN godep go install

# start it up
ENTRYPOINT ["ad-benchmarks"]
