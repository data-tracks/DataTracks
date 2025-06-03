<p align="center">
<img alt="Logo" height="300" src="/assets/logo/logo-transparent.png?raw=true" title="DataTrack" width="300"/>
</p>

# 🚂 DataTracks

### Blazingly fast stream processing that tracks!

DataTracks is a streaming engine, which simplifies the setup up and management of data stream processing, whether the data is structured or unstructured. It is aimed at developers and data engineers who need a reliable and scalable solution for handling diverse data types and streaming workloads.

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
```
{sql|SELECT $1 FROM $1} // transformation - what
```

```
...}[3s] // window - where
```

```
...}(f?) // layout
```

```
...}@watermark // trigger - when
```


```
{sql|SELECT $1 FROM $1}(f?)[3s]@watermark // all
```

## License

[GPLv3](https://www.gnu.org/licenses/)

