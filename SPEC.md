# Emit System

  [Event Specification] | [Python Emit]

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


## About

This is the central repository for documents related to the emit system. The emit system
was created to have a uniform way to track operations as they flow through various
components and systems. To achieve this a common [Event Specification] was created
and tooling is in the works to make use of the event data.


## Design

Applications convert existing workflows into a collection of immutable transactions in
the form of JSON and send them to an endpoint. The transactions all are identified by a
common transaction id `tid` field. This allows us to roll up a transaction log from the
emitted events. Each transaction is required to have an **open** and **close** event which
we may use to identify transactions that have not yet completed. For transactions that
span many components they are encouraged when resuming an existing transaction to mark
enter and exit events for further book keeping. Reentrant components are encouraged to
log the payload that would be needed to retry the request.


## Delivery

Applications emit the events using a common library with the ultimate destination for retrieval
being Elasticsearch and Hadoop.


## Features

The current [Event Specification] paves the way for the following features.


### Monitoring

This was the primary reason for the creation of the emit system. With each transaction having a definitive beginning and end we may easily determine transactions that are still **open** with no new activity within a certain threshold. This gives us visibility of an application outages scale leaving nothing to guess. Immediately identify if it's the entire system down, only a specific component.. and even identify if it's a specific region of code. Our existing monitoring solution Sensu is good for host level information and notification and the emit system will likely leverage it for notification. However it does not not have a sufficient design to provide much context for diagnostics. Being notified when a system is down is obviously important but the immediate next step is remediation. The emit system tries to add as granular of information as possible for diagnosing issues.


### Replaying Transactions

Through [Monitoring](#monitoring) we are aware of open transactions and where they are failing within our systems. As our systems have slowly became more and more decoupled into individual components (insert microservice buzzword here) they begin to desire and implement the property of being reentrant. Since they handle a small portion of a transaction and may be in the middle of multiple workflows they have to be prepared to accept work that has already been completed or would be an error to attempt again.These services handle a small part of the work they will often forward requests to the next component. If a component wishes to log the request at the **enter** event and the forwarded request at **exit** we may make use of this for a concept called [Replays](#replaying-transactions). The benefit of creating a replayable  exit is if the next component is currently down we have a way to replay any open transactions directly through it without waiting needing to reenter the component before it. Once an event is replayed and the next component comes up, this has the benefit of closing the currently open transactions once the component is properly servicing requests again.

Replayable events are emitted with a special field called **replay** which is a URI of a predefined replay client. This has not yet been flushed out but since most requests are http or can be broken down further into swagger API requests, the hope is to create common a common schema for replying events. This is likely best visualized with an example:

  > JSON:
  > ```json
  > {
  >   "tid": "84546fec-0661-41ac-bedb-3dff65903a5d",
  >   "time": "2016-01-01T10:10:10.123456-07:00",
  >   "system": "test.emit",
  >   "component": "test.api",
  >   "operation": "test.mitigation",
  >   "name": "enter",
  >   "replay": "test://data/incoming_request/replay_data",
  >   "data": {
  >     "...Many fields for debugging...": true,
  >     "incoming_request": {
  >       "replay_data": {
  >         "type": "mitigation",
  >         "resource": "127.0.0.1",
  >         "more_info": true
  >       }
  >     }
  >   }
  > }
  > ```


We can create other schemas that are more generic, such as:


  > JSON:
  > ```json
  > {
  >   "tid": "84546fec-0661-41ac-bedb-3dff65903a5d",
  >   "time": "2016-01-01T10:10:10.123456-07:00",
  >   "system": "test.emit",
  >   "component": "test.api",
  >   "operation": "test.mitigation",
  >   "name": "enter",
  >   "replay": "http://data/incoming_request/replay_data",
  >   "data": {
  >     "...Many fields for debugging...": true,
  >     "incoming_request": {
  >       "replay_data": {
  >         "method": "POST",
  >         "type": "json",
  >         "url": "http://foo.example.com/action",
  >         "headers": {
  >           "x-emit-tid": "84546fec-0661-41ac-bedb-3dff65903a5d"
  >         },
  >         "body": {
  >           "do_action": true,
  >         }
  >       }
  >     }
  >   }
  > }
  > ```


There may be a better way to define the replay field but this is an example and food for thought. With having a few common defined replay types and adding more as they come up.. any team or new components can quickly leverage the features replay enables without any sort of custom application specific logic needed. Existing tooling should just work. The replayable events could be done with a command line tool or be part of a UI. Having a common schema to replay events lays the foundation for some of the following tooling.


### Auto Remediation

With complex transactions that span multiple components these systems inherit a natural property of fault expectance. We will have visibility of these faults through [Monitoring](#monitoring) and it is likely at least one component within a transaction honors [Replayable](#replaying-transactions) events at either enter or exit. This gives us the opportunity to try automatically remediate open transactions through replaying events as close to the end of the current transaction logs as possible. If a given replay fails at the tail of the log, we could always try a level higher if possible. We may easily create basic logic to intelligently and without human intervention remediate outages. For example:

  - Define as an multiple of the expected average transactions per hour, derived from the percentage of the last 7 days of successful transactions.
  - For all open transactions for a given system, if within {THRESH_HOLD} replay each one a single time adding a `tag` and a data attribute with the replay number and time.
    - If an event has already been replayed, respect a basic exponential ease back and then try again from a transaction at a higher level in the workflow if it exists.
  - For all open transactions for a given system, when {THRESH_HOLD} has been exceeded.
    - Only attempt to replay a random sampling of a percentage of {THRESH_HOLD} size derived from a basic deterministic selection algorithm to make sure to ease the service into recovery.

Logic like above could be expanded, but just some basic rules like above provide reliable remediation. This also means that if a deployment introduced a change that broke a specific code path, the transactions will remain open and when a fix is deployed they may serve as the verification of a valid fix.


### Statistics and Analytics

With events having clear origins with time information it opens up the availability of some common statistics. Kibana dashboards can be created showing some base line information such as average transaction time by system, component, operation and many other combinations. We can analyze this information to find slow points in code and where handoffs experience high latency. Visualize inter-system relationships across teams to help integration efforts.. and so on.

Sometimes a request may not ever be recoverable, an alert/management interface could have a simple button which emits a **close** event for the transaction with a tag identifying human intervention. System developers may analyze these transactions and notice trends that are consuming large amounts of human time and determine the best remediation course. Analyzing this data to determine pain points will provide a lot of value.


### Heartbeats

With all events being [Replayable](#replaying-transactions) and the ability for [Auto Remediation](#auto-remediation) we can create a concept of Heartbeats as a known set of real production requests that should always result in a completed transaction. We can create a simple scheduling engine to produce a **pulse** through systems. There are a lot of possible designs, but I think it would be best to emit "events" for replays as well. We have a reliable storage system (ES) and it would be easy to create a basic scheduling service selects all events from elasticsearch with a specific tag, i.e. `heartbeat`. If that event is a valid replayable event occasionally send it through the system with some additional book keeping data. The [Monitoring](#monitoring) system will automatically pick up any failed heartbeats and will be outside the concern of the hearbeat scheduler. By reusing the same pipeline we get all the benefits of it to safeguard from regressions.

Creating a known set of heartbeats and implementing some basic logic in the scheduler we can make sure that systems which have brief periods of idle time between bursts of service are always healthy. Don't need to wait until a service is needed to know it has entered an unhealthy state. By using the same system as what is in production it promotes best practices in service design and allows each feature of the emit system to be fully leveraged.


### CLI

Some really basic tooling can provide some powerful benefits here. Imagine a collection of simple cli utilities. Used together to quickly create some heartbeats for a given issue from the day before to make sure we protect against it for a given amount of time. We can use the **data** field to store meta data for the scheduler to refresh the heartbeats by reinserting into ES so they do not roll off the index. Creating a basic set of cli programs that cooperate through stdin / stdout and reading emit events you could chain them together and have them picked up by various components of the emit system, such as the auto replays or heartbeats. It could be one large binary or a collection of binaries. They could be distributed in a pre-configured docker container all available within the current environment for easy upstart.

Some example workflows:

  - Fetch all transactions that were open for more then 1 hour the day before matching a filter (similar to [ES Check]):

    > Command:
    > ```bash
    > fetch --last 24h --duration 1h -s test.emit -c api -o mitigation \
    >   | filter --include 'event contains replay' \
    >   | filter --exclude 'event[data][error_message] contains MyValue' \
    >     > results.json
    > cat results.json
    > ```
    >
    > Output:
    > ```json
    > {
    >   "result": true,
    >   "events": [
    >     {
    >       "tid": "84546fec-0661-41ac-bedb-3dff65903a5d",
    >       "time": "2016-01-01T10:10:10.123456-07:00",
    >       "system": "test.emit",
    >       "component": "test.api",
    >       "operation": "test.mitigation",
    >       "name": "enter",
    >       "replay": "http://data/incoming_request/replay_data",
    >       ...
    >     },
    >     ...
    >   ]
    > }
    > ```


  - The data looked good, convert those events to [Heartbeats](#heartbeats):

    > Command:
    > ```bash
    > cat results.json \
    >   | select --skip 5
    >   | heartbeat --expires 24w \
    >     > heartbeats.json
    > cat heartbeats.json
    > ```
    >
    > Output:
    > ```json
    > {
    >   "result": true,
    >   "events": [
    >     {
    >       "data": {
    >         "heartbeat": {
    >           "expires": "2016-03-01T10:10:10.123456-07:00",
    >           ... other metadata
    >         }
    >       },
    >       "tags": [
    >         "heartbeat"
    >       ]
    >     },
    >     ...
    >   ]
    > }
    > ```


  - They are all failing, oops, lets remove them for now. Note that a delete would really just emit new events with an expiration date in the past or a special flag to ignore them. Tooling would always just respect the latest version of a given message.

    > Command:
    > ```bash
    > cat heartbeats.json | delete
    > ```
    >
    > Output:
    > ```json
    > {
    >   "result": true,
    >   "deleted": [
    >     "tid1", "tid2", ...
    >   ]
    > }
    > ```


## Bugs and Patches

  Feel free to report bugs and submit pull requests.

  * bugs:
    <https://github.com/godaddy/py-emit/issues>
  * patches:
    <https://github.com/godaddy/py-emit/pulls>


## Related

  - [Python Emit]


[Python Emit]: https://github.com/godaddy/py-emit
[Event Specification]: https://github.com/godaddy/py-emit/blob/master/EVENT.md
