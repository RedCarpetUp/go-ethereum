FROM debian:jessie

RUN apt-get update
RUN apt-get install -y git
RUN apt-get install -y build-essential
RUN apt-get install -y wget

RUN wget https://redirector.gvt1.com/edgedl/go/go1.9.2.linux-amd64.tar.gz
RUN tar -C /usr/local -xzf go1.9.2.linux-amd64.tar.gz

RUN git clone https://github.com/RedCarpetUp/go-ethereum.git --branch dev --single-branch 

ENV GOPATH /go
ENV GOROOT /usr/local/go
ENV PATH /usr/local/go/bin:/go/bin:/usr/local/bin:$PATH


WORKDIR /go-ethereum

RUN go version
RUN make geth
RUN build/bin/geth
