# General

*	Run with memory debugging tools to check memory safety
*	Clean up documentation
*	Clean up enums to use new capitalization style
*	Add support for using database options and network options

# Database Connection

*	Make InMemoryDatabaseConnection thread-safe
*	Consider purging old version history in InMemoryDatabaseConnection
*	Consider adding support for streaming results for range reads
*	Add documentation for FDB API enums
*	Support more atomic operations in InMemoryConnection
*	Support for transaction options in InMemoryConnection
*	Support snapshot reads in InMemoryConnection
*	Capture errors from adding conflicts
*	Capture errors from setting transaction options

# Documentation

*	Add documentation pages for the major components.

# Testing

*	Test tuple system against other bindings
*	Revisit acceptance tests for the database connections
*	Set up CI

# Binding Tester

*	Add more unit tests
*	Add support for more snapshot and database variants
*	Add tests for more snapshot and database variants
*	Add more tests for the GetRangeSelector commands 
*	Test catching and wrapping FDB API errors
*	Check which commands optionally push futures to see if there's cases where
	we're pushing nothing.
*	Make transaction map shared between parallel binding tester runs
*	Make binding tester push null values when the push command has a null
	argument

# Tuple Enhancements

*	Versionstamp values
*	Add unit tests for new helper methods
*	Clean up references to ParsingError in documentation
*	Consider reworking the TupleConvertible type so that it doesn't need an associated
	type requirement.
*	Supporting optional values
