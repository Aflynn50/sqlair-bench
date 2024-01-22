FROM ubuntu:22.04

EXPOSE 3333

COPY ./sqlair-bench /sqlair-bench

RUN apt-get update && apt-get install -y --no-install-recommends \
  && apt-get install -y libdqlite-dev 

ENTRYPOINT ["/sqlair-bench"]
