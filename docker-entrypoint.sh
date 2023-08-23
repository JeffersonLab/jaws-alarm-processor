#!/bin/sh

echo "-------------------------------------------------------"
echo "Step 1: Waiting for Schema Registry to start listening "
echo "-------------------------------------------------------"
while [ $(curl -s -o /dev/null -w %{http_code} http://registry:8081/schemas/types) -eq 000 ] ; do
  echo -e $(date) " Registry listener HTTP state: " $(curl -s -o /dev/null -w %{http_code} http://registry:8081/schemas/types) " (waiting for 200)"
  sleep 5
done

export JAWS_EFFECTIVE_PROCESSOR_OPTS=-Dlog.dir=/opt/jaws-effective-processor/logs
/opt/jaws-effective-processor/bin/jaws-effective-processor &

sleep infinity
