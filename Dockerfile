FROM ubuntu:22.04

EXPOSE 3333

COPY ./sqlair-bench /sqlair-bench

ENTRYPOINT ["/sqlair-bench"]
