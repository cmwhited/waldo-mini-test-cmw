#! /usr/bin/env sh

./scripts/wait-for.sh postgres:5432 -- echo "postgres is up"
./scripts/wait-for.sh rabbitmq:5672 -- echo "rabbitmq is up"

# to run web interface, uncomment this line
python src/services/web.py
# to run message consumer, uncomment this line
# python src/services/message_consumer.py