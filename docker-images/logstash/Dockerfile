ARG ELASTIC_VERSION

FROM docker.elastic.co/logstash/logstash:${ELASTIC_VERSION}

# Copy your Logstash configuration file
COPY ./config/logstash.yml /usr/share/logstash/config/

# Set the correct ownership and permissions
USER root
RUN chown -R logstash:logstash /usr/share/logstash/config/
USER logstash
