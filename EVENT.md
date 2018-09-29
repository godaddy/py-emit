# Event Specification

  [Emit System] | [Python Emit] | [Go Emit]

  > ```json
  > {
  >   "tid": "84546fec-0661-41ac-bedb-3dff65903a5d",
  >   "time": "2016-01-01T10:10:10.123456-07:00",
  >   "system": "test.emit",
  >   "component": "emitter",
  >   "operation": "ping",
  >   "name": "open"
  > }
  > ```


# Emit Events

This is the initial draft to describe the **emit** events system. This is a living document and I encourage everyone to edit / leave comments. Currently the event data structure is considered frozen. Keys will never change though additional keys may be added they *SHOULD* not be required.


## Overview

The purpose of this system is to create a rich transaction log for the lifecycle of system operations. Each transaction must have at least two occurrences to be useful, an `open` (or begin) and `close` (or end) event. This is to mark the transaction so consumers of this pipeline can identify open transactions. This transaction log will have multiple component operations capable of creating an `open` event. The system doesn't care which component operation `open`'s the transaction and it will remain flexible as the system changes over time. The various system components may create as many events per operation as needed in order to derive a detailed lifecycle when the transaction log is rolled up. Each component operation should be encouraged (but not required) to emit an `enter` and `exit` event so we may identify failure points with as much granularity as possible. In addition if the components operation is reentrant it may include the payload it received in the `enter` event. If the component is in front of a device / black box including the payload for the handoff would be valuable as well. I don't think it should be a problem for a component to emit multiple events of the same name and in fact I think it would roll up nicely using just the timestamp for things like queue workers or stuff that tends to fail and retry often.

This does create the demand for a common identifier for every transaction to be maintained across components. There is a few ways you can go about this and depends on the needs of the implementing system, but you are not restricted to maintaining an inter-component "handle". Some options:

  * You may derive a RFC 4122 v4 uuid (random data uuid) at the beginning of the transaction and pass it from touch point to touch point.
  * You may derive a RFC 4122 v5 uuid with the existing "shared" state that is already used at hand off points to rebuild the transaction id at each component in a deterministic way.


### Rationale

Once we have this transaction log we can use it to identify mitigations that were dropped with immediate precision. If the identified failure point is reentrant and the component provides a payload, we can replay the `event` by pushing the payload back through. If this replay succeeds it has the natural implication of closing the transaction, ending the `alert` status. If there was a known temporary outage this transaction log could be replayed en masse when the system's primary fault counter measures fail.

Another added benefit of this design is it allows us to easily create real time `heartbeats` that are always creating a pulse through the system. These heartbeats can be scheduled in intervals for any component in the system with an expected result. Leveraging the production pipeline for this we will assert we are providing a constant service level against a known set of requests at all times. As edge cases slip through the cracks and are fixed we can produce heartbeats for them with some basic information about why they were created as part of remediation. This and replays do create a possible demand to sub categorize an event to separate it from natural transactions in a more concrete way.


## Requirements

Limiting events to key / value pairs makes the most sense to me, I would like to see if we can avoid too much discussion around typing or having rigid schemas. Just be JSON, with standard json types, string, number, object, array, bool and null. I propose for dates, UTC time using RFC 3339 and let's agree to a fractional precision (I +1 for 6) now if possible.


### Fields and Elasticsearch

All emit events are stored within Elasticsearch. This means we must respect a few details about how Elasticsearch indexes data. As described in detail at [Field Mapping datatypes], each value within the document you send to Elasticsearch must be associated to a type that it knows how to index. How Elasticsearch maps json documents to Mapping types is explained at [Mapping Intro Guide]. Each document keys canonical location may only be associated to a single Mapping type. There was a time when a key could have many types, but then [The Great Mapping Refactoring] happened.

This means that if in one system you were to emit an event such as:

  ```json
  {
    "system": "system_one",
    ...
    "fields": {
      "error": "Error string",
    }
  }
  ```

Then a couple days later, a new system was to attempt to emit the same exact canonical key location with a different type:

  ```json
  {
    "system": "system_two",
    ...
    "fields": {
      "error": 12345,
    }
  }
  ```

It would result in permanent message failure. This is because `fields.error` has already been established in the first document as a *string* type. These mapping issues are particularly bad because they may not get detected until the very last hop of the workflow which will leave the end user unaware the messages have been discarded. Below is a table of how VALID *JSON* is converted to it's associated Elasticsearch *Mapping Type*:

  - JSON String: "foo" -> ES Mapping: string
  - JSON Boolean: true or false -> ES Mapping: boolean
  - JSON Whole number: 123 -> ES Mapping: long
  - JSON Floating point: 123.45 -> ES Mapping: double
  - JSON String, valid date: 2014-09-15 -> ES Mapping: date
  - JSON Array of string: [ "foo", "bar", "baz" ] -> ES Mapping: array of strings

This means that all fields must follow the type requirements within this document. For fields that are defined by the user they must exist in the `fields` or `data` keys.

The `data` field is NOT indexed by Elasticsearch so you may put anything you would like to in this. It will not be searchable, but you may retrieve it later.

The `fields` field is indexed by Elasticsearch and may be searched. In order to allow this safely, fields must share the same types. Any field that exists within the `fields` key *MUST* have a suffix of it's associated Elasticsearch type. There is two special cases, one is that string values may be a bare word as that is the default field type for this specification. A second special case is array types need to have a suffix of the type which they contain. Example field names for their associated Elasticsearch types:

  - [String datatype] { "foo": "bar" }
  - [String datatype] { "foo_string": "bar" } # valid but superfluous
  - [Numeric datatypes] { "foo_long": 12345 }
  - [Date datatype] { "foo_date": "2016-04-13T15:10:05.123456-07:00" }
  - [Boolean datatype] { "foo_boolean": true}
  - [Array datatype]  { "foo_arr_long": [ 123, 1234, 12345 ] }
  - [Binary datatype] *Not yet used*
  - [Object datatype] *Not yet used*
  - [Nested datatype] *Not yet used*


### Special Names

Tooling will ignore all events which have the prefix `test.` within any of the following
fields: *system*, *component*, *operation*, *tid*. They will still be sent to Elasticsearch and you
may query for them directly. Only the emit system tooling will filter those events out.


### Event Structure

Here is a list of `key` => `value` requirements by functional demand, doesn't mean they should have inclusion in the structure as displayed here by name or canonical location. I.E. Maybe `tags`, and `replay` lives inside the `data` object.


  - tid

    > String *tid* representing the transaction id. It is the only immutable constant within a transaction and should be unique to the system. It should be passed at each touch point originating when the transaction is opened. I suggest RFC 4122 it has a few different generation methods, maybe one of them could fit our needs. UUID Generation utilities exist under most os distributions as `uuidgen` and there are standard libraries in most every language.
    >
    > I.e.
    >   84546fec-0661-41ac-bedb-3dff65903a5d


  - timestamp

    > Having the components time synchronized will be important. The timestamp is relative to the point in the transaction which it occurs. Will be used for rolling up transactions into measurable timeline statistics that can be shipped to graphite, or used in various out of band analytics like mitigation response times and identifying high latency points in the system. It also combined with the UUID, system, component and event gives us a pretty safe per-event unique handle where needed. I think a string RFC 3339 formatted date in UTC would be good.
    >
    > I.e.
    >   2016-04-13T15:10:05.123456-07:00


  - system

    > String *identifier* representing the name of the entire system. If we ever share this spec maybe we could also define what an identifier is via a basic regex. I personally dislike dashes (language support)
    >
    > I.e.
    >   `mysystem`


  - component

    > String *identifier* representing the name of a component within a system.
    >
    > I.e.
    >   `worker`, `web`, `api`


  - operation

    > String *identifier* representing the operation a component is performing. If it is not provided a library may use the component name.
    >
    > I.e.
    >   `delete`, `create`, `list`


  - name

    > String *identifier* representing the name of an interesting occurrence within a components current operation.
    >
    > I.e.
    >   `open`, `close`, `enter`, `exit`, `queued`, `processing`, `blocked`, `pending`, `failed` .. anything.


  - replay

    > Replaying events for reentrant components would be nice, if we choose to do that it would we may provide a mapping of "how to" replay an event to get as much as we can from a simple quick interface / dashboard. Defining a few common transports could give us a lot for free later. Just a thought. It wouldn't add much overheard to the message, just a simple transport identifier like `tcp | http | tls | amqp | custom | etc..` then transport specific details that fall outside a set of sane defaults. The benefit here is authenticated systems can have their own transport handle that can be granted "replay" rights.
    >
    > We could also have it be a more basic string key and roll it up in uri form. This should probably work for most use cases including. For example test.emit://blackhole:create/data/payload could cause a blackhole to be created using the events data/payload key.
    >
    > I.e.
    >   - transport: http
    >   - port: 5801
    >   - headers: <additional k:v of headers>
    >   - method: <if it's not a sane default of.. post if there is a payload, get if null? (be sane)>
    >   - payload: post-data


  - tags

    > We may want to quickly distinguish events from multiple sources. For example we will want to know if the origination of an event is a heartbeat or replay. It may be convenient to facilitate this through a small array of tags.
    >
    > I.e.
    >   [ `replayed`, `heartbeat`, `unittest` etc... ]


  - fields

    > A mapping of key value pairs that must conform to a strict naming convention in order to circumvent some shortcomings within ElasticSearch, as mentioned above [Fields and Elasticsearch].
    >
    > *Currently this field may not contain nested objects*
    >
    > I.e.
    > ```json
    >   {
    >     "error": "Error string",
    >     "error_boolean": True,
    >     "error_long": 12345,
    >     "error_double": 12345.6789,
    >     "error_date": "2016-04-25T17:52:37.123456Z",
    >     "error_array_long": [
    >       1, 2, 3, 4, 5
    >     ],
    >     "error_array": [
    >       "string1", "string2", "string3"
    >     ]
    >   }
    > ```

  - data

    > General bag-o-data that is NOT indexed or searchable. Components may want to provide extra data (i.e. stack trace) to better understand an interesting occurrence like permanent failure. This does not have to follow the strict requirements of *fields* which makes it a nice place to simply dump a structure for debugging without caring about ES Mapping conflicts.
    >
    > I.e.
    > ```json
    >   { "error": "Error string" }
    > ```
    >
    > ```json
    >   { "error": 12345 }
    > ```
    >
    > ```json
    >   { "error": [ "string1", "string2", "string3" ] }
    > ```
    >
    > ```json
    >   { "error": [ 1, 2, 3, 4, 5 ] }
    > ```


### Event Data: Encoding

The encoding by default should be JSON where possible, but since it is simply key value pairs with very minimal typing requirements there is no reason you can't encode it in something else.


#### JSON Encoded Event Examples

Basic example:

  ```json
  {
    "tid": "84546fec-0661-41ac-bedb-3dff65903a5d",
    "time": "2016-01-01T10:10:10.123456-07:00",
    "system": "test.emit",
    "component": "test.api",
    "operation": "test.mitigation",
    "name": "enter"
  }
  ```

Full example:

  ```json
  {
    "tid": "84546fec-0661-41ac-bedb-3dff65903a5d",
    "time": "2016-04-13T15:10:05.123456-07:00",
    "system": "test.emit",
    "component": "test.middleware",
    "operation": "test.mitigation",
    "name": "enter",
    "tags": [ "my_tag" ],
    "fields": {
      "error": "Error string",
      "error_boolean": True,
      "error_long": 12345,
      "error_double": 12345.6789,
      "error_date": "2016-04-25T17:52:37.123456Z",
      "error_array_long": [
        1, 2, 3, 4, 5
      ],
      "error_array": [
        "string1", "string2", "string3"
      ]
    },
    "data": {
      "error": "Error string",
      "error": True,
      "error": 12345,
      "error": 12345.6789,
      "error": "2016-04-25T17:52:37.123456Z",
      "error": [
        1, 2, 3, 4, 5
      ],
      "error": [
        "string1", "string2", "string3"
      ]
    }
  }
  ```

Replayable event:

  ```json
  {
    "tid": "84546fec-0661-41ac-bedb-3dff65903a5d",
    "time": "2016-04-13T15:10:05.123456-07:00",
    "system": "test.emit",
    "component": "test.middleware",
    "operation": "test.mitigation",
    "name": "enter",
    "replay": "test://data/request",
    "data": {
      "request": {
        "transport": "test",
        "resource": "blackhole",
        "action": "create",
        "payload": {}
      }
    }
  }
  ```


## References

Collection of links related to this specification.


### General

  - [The Great Mapping Refactoring]
  - [Mapping Intro Guide]
  - [Field Mapping datatypes]
  - [String datatype]
  - [Numeric datatypes]
  - [Date datatype]
  - [Boolean datatype]
  - [Binary datatype]
  - [Array datatype]
  - [Object datatype]
  - [Nested datatype]


[Python Emit]: https://github.com/godaddy/py-emit/issues
[Emit Specification]: https://github.com/godaddy/py-emit/blob/master/SPEC.md
[The Great Mapping Refactoring]: https://www.elastic.co/blog/great-mapping-refactoring#conflicting-mappings
[Mapping Intro Guide]: https://www.elastic.co/guide/en/elasticsearch/guide/current/mapping-intro.html
[Field Mapping datatypes]: https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-types.html
[String datatype]: https://www.elastic.co/guide/en/elasticsearch/reference/current/string.html
[Numeric datatypes]: https://www.elastic.co/guide/en/elasticsearch/reference/current/number.html
[Date datatype]: https://www.elastic.co/guide/en/elasticsearch/reference/current/date.html
[Boolean datatype]: https://www.elastic.co/guide/en/elasticsearch/reference/current/boolean.html
[Binary datatype]: https://www.elastic.co/guide/en/elasticsearch/reference/current/binary.html
[Array datatype]: https://www.elastic.co/guide/en/elasticsearch/reference/current/array.html
[Object datatype]: https://www.elastic.co/guide/en/elasticsearch/reference/current/object.html
[Nested datatype]: https://www.elastic.co/guide/en/elasticsearch/reference/current/nested.html
