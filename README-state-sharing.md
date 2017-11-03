No guarantee of being up-to-date...

State sharing enables a global state view of the queryable key-value registered states.
The changes introduced in this fork enable operators to GET and SET any registered internal state at runtime.

Here is the CHANGELOG:

  * Had to introduce a reference for the QueryableStateProxy from the underlying Task to the RuntimeEnvironment
    for operator accessibility;
  * Wrapped The logic of response to external query of the proxy in a GlobalStateGateway that can perform requests
    programmatically to TaskManagers;
  * Thanks to the reference to the proxy available in the RuntimeEnvironment, operators can require a GlobalStateClient
    (built around the GlobalStateGateway) to query programmatically query other operator's state.

### TODO

  * Implement SETting other operator's keys: this will require to create new flavors of InternalKvStateRequests
    (flink-queryable-state);
  * Fix wrapping of querying logic in the proxy. It is not good by now...;
  * Could have a better API for the GlobalStateClient (optional);
  * Integrate the code for transactionality: this will require to create specific services for transaction coordination!


