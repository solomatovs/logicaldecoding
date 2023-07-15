FROM postgres:14

RUN apt update && apt install -y \
  postgresql-14-decoderbufs \
  postgresql-14-wal2json
