# Small scale stream processing kata (concurrent/parallel programming)

Thanks, Tomasz for [inventing this kata](https://nurkiewicz.com/2016/10/small-scale-stream-processing-kata-part.html).

A system delivers around one thousand events per second. Each Event has at least two attributes:
- `clientId` - we expect up to few events per second for one client
- `UUID` - globally unique

Consuming one event takes about 10 milliseconds. Design a consumer of such stream that:
- allows processing events in real time
- events related to one client should be processed sequentially and in order, i.e. you can not parallelize events for the same `clientId`
- if duplicated `UUID` appeared within 10 seconds, drop it. Assume duplicates will not appear after 10 seconds

There are few important details in these requirements:
- 1000 events/s and 10 ms to consume one event. Clearly we need at least 10 concurrent consumers in order to consume in near real-time.
- Events have natural aggregate ID (`clientId`). During one second we can expect a few events for a given client and we are not allowed to process them concurrently or out of order.
- We must somehow ignore duplicated messages, most likely by remembering all unique IDs in last 10 seconds. This gives about 10 thousand UUIDs to keep temporarily.
