# Homework 6

## Questions
1. Version of redpanda:
  - Ran `docker exec -it redpanda-1 bash`. Then ran `rpk version` inside the shell
  - `v24.2.18`
2. Create Topic
  - Ran `rpk topic create green-trips`
  - Output: `TOPIC: green-trips  STATUS: OK`
3. Connecting to Kafka Server
  - Ran code in `test_kafka.ipynb`
  - Output: `True`
4. Sending the Trip Data
  - Ran code in `test_kafka.ipynb`
  - Output: `took 11.86 seconds`
5. Build a Sessionization Window 
  - 