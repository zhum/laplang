GOFLAGS=-ccflags -g

GOPATH=/home/serg/go

all:
	GOPATH=$(GOPATH) go build $(GOFLAGS)
