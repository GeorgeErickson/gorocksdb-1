package event

/*

How to use MultiIterator to aggregate events from different topics?
We use the TopicEventEventKey from github.com/kapitan-k/goutilities/event
as keys which have a fixed length/size of 16 byte
8 byte uint64 for the topic ID and
8 byte uint64 for the event ID, which includes time micros.

See package example.

*/
