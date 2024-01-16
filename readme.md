# Distributed Map Reduce

## Introduction

This repository contains the implementation of a distributed map-reduce system. The architecture utilizes sockets and threads, with three main components: Master, Mappers, and Reducers. Users can specify the number of mappers, reducers, and the desired application (word count or inverted index) when running the program.

## Architecture

The architecture is designed with the Master acting as the coordinator, initiating the specified number of mapper threads. These mapper threads process distributed data and filter it through a hash function before sending it to reducers. Reducers, in turn, listen for data from mappers, aggregating and storing the results in JSON format in distributed file paths.

## Design

### 1. Master (master.py)

The Master serves as the coordinator, responsible for initializing mappers, acknowledging completion messages from mappers, and initiating reducers.

### 2. Mapper (mapper.py)

Mapper threads process chunks of data, calculating word count or generating inverted indices. They filter data through a hash function and send it to reducers through sockets. The goal is to simulate a distributed environment without using intermediate storage.

Functions:
- `calculateWordCount`: Calculates word count.
- `calculateInvertedIndex`: Generates inverted index.
- `mapperSend`: Sends data to reducers.
- `runMapper`: Executes the mapper thread.

### 3. Reducer (reducer.py)

Reducer threads, spawned by the master, receive data from mappers, aggregate results, and store them in an output file.

Functions:
- `sendToReducer`: Sends data to the reducer.
- `sendDataToReducer`: Handles data reception and storage in the output file.

### 4. Data Partitioning

Input data is divided so that all mappers receive an equal share if the number of files is divisible evenly. The last mapper handles any extra load. The program also handles cases where a mapper may not receive any file, in which case the master spawns only the required mapper nodes.

