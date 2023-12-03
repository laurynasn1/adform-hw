# Adform homework task

## Requirements

Java 14+, Maven, Docker Compose

## Preparation

To build application jar and start Kafka container run `prepare.sh`.

## Running application

To run the application, execute `run.sh <user agent> <path to data folder>`.

NOTE: If `<user agent>` has spaces in it, it must be enquoted, i.e. `"some user agent"`.

## Reading results

To read processed results, run `read_results.sh`. It uses Kafka Consumer to read all output topic records and prints them in readable format.

## Cleanup

To stop Kafka container and delete result data, run `clean.sh`.