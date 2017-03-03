FROM golang:1.8
MAINTAINER Krishna Kunapuli <krishna_kunapuli@starpathit.com>

# setup env
ENV projectdir $GOPATH/src/github.com/kunapuli09/ad-benchmarks/
ENV zookeepers "10.0.0.131:2181"
ENV reset_offsets "false"
ENV events_topic "ad-events"
ENV group_id "ad-golang"
ENV workers 5
ENV flush_interval "500ms"
ENV restart_interval "5m"
ENV redis_url "http://localhost:6379"
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
