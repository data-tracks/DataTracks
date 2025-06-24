<p align="center">
<img alt="Logo" height="300" src="/assets/logo/logo-transparent.png?raw=true" title="DataTrack" width="300"/>
</p>

# 🚂 DataTracks

### Blazingly fast stream processing that tracks!

DataTracks is a streaming engine, which simplifies the setup up and management of data stream processing, whether the
data is structured or unstructured. It is aimed at developers and data engineers who need a reliable and scalable
solution for handling diverse data types and streaming workloads.

## Features

- **Easy Setup**: Quickly set up data streams with minimal configuration.
- **Flexibility**: Supports both structured and unstructured data in various data formats.
- **Prototype System**: Currently in the prototype stage, providing a foundational system for further development.

DataTracks is a prototype engine, which provides simple creation and management of complex data streaming workloads for
various degrees of structured and unstructured data.

## Getting Started

A simple plan which restructures an input MQTT stream and sends it forward might look like this:

```
1--2{sql|SELECT {time: $1.timestamp, id: $1.id} FROM $1}--3

In
MQTT{...}:1

Out
MQTT{...}:3
```

## Documentation

DataTracks uses an abstraction model based on train and track logic.

### Trains

In its model a data point is represented by a ```Wagon```, which gets assigned timestamp on initialization at the
client (wagon number).
Multiple ```Wagons``` can be connected to one another as a ```Train```.
```Trains``` travel a track ```Plan```, which defines how different components are connected.
Each component is represented by a ```Stations```.

```Stations``` can receive and send out ```Trains```. If a ```Train``` passes through a ```Station```
it has tho conform to the structural requirement of the station (Who?).
```Trains``` might are collected at a ```Station``` and grouped into time windows according to their train number (
Where?).
Further ```Trains``` might be held back until a defined condition is reached (When?).

Due to counterparties there might be the a late ```Train``` arriving later at the station (How?).

### What - Transformation

```
{sql|SELECT $1 FROM $1}
```

### Where - Window (event-time)

```
...}[3s]
```

### Who - Layout

```
...}(f?) //nullable
...}(s') //optional
...}({name: s'}) // document with key name of optional string type

```

### When - Trigger (processing-time)

```
...}@windowEnd
...}@element
...}@windowNext
...}@windowNext@element // trigger on next window and on element
```

### Example

```
{sql|SELECT $1 FROM $1}(f?)[3s]@watermark // all
```

### SQL Syntax

```sql
SELECT *
FROM $0
WHERE id == "TEST"
GROUP BY "salary"
    WINDOW INTERVAL (5, SECONDS)
    EMIT ELEMENT, WINDOW (END)
    MARK LAST
```

## License

[GPLv3](https://www.gnu.org/licenses/)

