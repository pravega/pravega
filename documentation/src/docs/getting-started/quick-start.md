# Quick Start Guide

## Download the latest Pravega
```
wget https://github.com/pravega/pravega/releases/download/v0.9.0-rc0/pravega-0.9.0.tgz
tar zxvf pravega-0.9.0.tgz
cd pravega
```

## Start Pravega standalone cluster
```
./bin/pravega-standalone
```

## Create your scope and your stream
```
./bin/pravega-cli
> scope create my-scope
> stream create my-scope/my-stream
```

## Write some unordered events to a stream
```
> stream append my-scope/my-stream 5
```

## Read those events from your stream
```
> stream read my-scope/my-stream
q
```

## Write and read some ordered events
```
> stream create my-scope/ordered-stream
> stream append my-scope/ordered-stream my-routing-key 5
> stream read my-scope/ordered-stream
q
```

## Shutdown
pravega-cli:
```
> exit
```
pravega-standalone:
```
Ctrl+C
```

## Next steps

In completing this guide, you have launched Pravega, created a scope, created a stream, written events to the stream, and read them back from the stream.

To use Pravega in real world applications, you’ll want to explore:
* Read some specific docs (link somewhere, say something)
* Something else, but I don’t know what yet
* Maybe some links to join the community or to presentations
