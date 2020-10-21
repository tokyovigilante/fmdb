import CSQLite
import Foundation
import Logging

let logger = Logger(label: "com.testtoast.fmdb")

/**
Enumeration used in checkpoint methods.
*/
public enum FMDBCheckpointMode: Int {
    case passive  = 0 // SQLITE_CHECKPOINT_PASSIVE,
    case full     = 1 // SQLITE_CHECKPOINT_FULL,
    case restart  = 2 // SQLITE_CHECKPOINT_RESTART,
    case truncate = 3  // SQLITE_CHECKPOINT_TRUNCATE
}

public enum SQLiteError: Error, LocalizedError {
    case database(message: String)

    public var errorDescription: String? {
        switch self {
        case .database (let message):
            return "\(String(describing: self)): \(message)"
        }
    }
}


/** A SQLite ([https://sqlite.org/](https://sqlite.org/)) Objective-C wrapper.

 Usage

 The three main classes in FMDB are:

 - @c FMDatabase - Represents a single SQLite database.  Used for executing SQL statements.

 - @c FMResultSet - Represents the results of executing a query on an @c FMDatabase .

 - @c FMDatabaseQueue - If you want to perform queries and updates on multiple threads, you'll want to use this class.

 See also

 - @c FMDatabasePool - A pool of @c FMDatabase objects

 - @c FMStatement - A wrapper for @c sqlite_stmt

External links

- [FMDB on GitHub](https://github.com/ccgus/fmdb) including introductory documentation
- [SQLite web site](https://sqlite.org/)
- [FMDB mailing list](http://groups.google.com/group/fmdb)
- [SQLite FAQ](https://sqlite.org/faq.html)

@warning Do not instantiate a single @c FMDatabase  object and use it across multiple threads. Instead, use @c FMDatabaseQueue .

*/

///---------------------
/// @name Initialization
///---------------------

/** Initialize a @c FMDatabase  object.

An @c FMDatabase  is created with a local file URL to a SQLite database file.
This path can be one of these three:

1. A file system url.  The file does not have to exist on disk.  If it does not
exist, it is created for you.

2. A file system url initialised with a zero-length string.  An empty database
is created at a temporary location.  This database is deleted with the
@c FMDatabase  connection is closed.

3. @c nil .  An in-memory database is created.  This database will be destroyed
with the @c FMDatabase  connection is closed.

For example, to open a database in the app's “Application Support” directory:

@code
NSURL *folder  = [[NSFileManager defaultManager] URLForDirectory:NSApplicationSupportDirectory inDomain:NSUserDomainMask appropriateForURL:nil create:true error:&error];
NSURL *fileURL = [folder URLByAppendingPathComponent:@"test.db"];
FMDatabase *db = [[FMDatabase alloc] initWithURL:fileURL];
@endcode

(For more information on temporary and in-memory databases, read the sqlite documentation on the subject: [https://sqlite.org/inmemorydb.html](https://sqlite.org/inmemorydb.html))

@param url The file @c NSURL  of database file.

@return @c FMDatabase  object if successful; @c nil  if failure.

*/
public class FMDatabase {

    // MARK: - FMDatabase Private Extension
    private var _db: OpaquePointer? = nil

    private var _isExecutingStatement = false
    fileprivate var _startBusyRetryTime: TimeInterval = 0
    private var _openResultSets = Set<FMResultSet>()
    //private var _openFunctions = Set()
    private var _dateFormat: DateFormatter? = nil

    private var sqlitePath: String? {

        guard let url = databaseURL else {
            return ":memory:"
        }
        return url.absoluteString
    }

    public var logsErrors = true
    public var crashOnErrors = false

    private (set) public var cachedStatements = [String: NSMutableSet]()

    public init (url: URL?) {
        assert(sqlite3_threadsafe() != 0, "SQLite is not threadsafe, aborting")
        databaseURL = url
     }

    ///-----------------------------------
    /// @name Opening and closing database
    ///-----------------------------------

    /// Is the database open or not?
    private (set) public var isOpen = false

    /** Opening a new database connection with flags and an optional virtual file system (VFS)

     @param flags One of the following three values, optionally combined with the @c SQLITE_OPEN_NOMUTEX , @c SQLITE_OPEN_FULLMUTEX , @c SQLITE_OPEN_SHAREDCACHE , @c SQLITE_OPEN_PRIVATECACHE , and/or @c SQLITE_OPEN_URI flags:

    @code
    SQLITE_OPEN_READONLY
    @endcode

     The database is opened in read-only mode. If the database does not already exist, an error is returned.

    @code
    SQLITE_OPEN_READWRITE
    @endcode

     The database is opened for reading and writing if possible, or reading only if the file is write protected by the operating system. In either case the database must already exist, otherwise an error is returned.

    @code
    SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE
    @endcode

     The database is opened for reading and writing, and is created if it does not already exist. This is the behavior that is always used for @c open  method.

     @return @c YES if successful, @c NO on error.

     @see [sqlite3_open_v2()](https://sqlite.org/c3ref/open.html)
     @see open
     @see close
     */

    /** Closing a database connection

     @return @c YES if success, @c NO on error.

     @see [sqlite3_close()](https://sqlite.org/c3ref/close.html)
     @see open
     @see openWithFlags:
     */
    public func open (flags: Int32 =
            SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI,
            vfs: String? = nil) -> Bool {

        if isOpen {
            return true
        }

        // if we previously tried to open and it failed, make sure to close it before we try again
        if _db != nil {
            _ = close()
        }

        let err = sqlite3_open_v2(databaseURL?.absoluteString, &_db, flags, vfs)
        if err != SQLITE_OK {
            logger.error("error opening!: \(err)")
            return false
        }

        if maxBusyRetryTimeInterval > 0 {
            // set the handler
            setMaxBusyRetryTimeInterval()
        }

        isOpen = true
        return true
    }


    public func close () -> Bool {

        clearCachedStatements()
        closeOpenResultSets()

        /*    if (!_db) {
                return YES;
            }

            int  rc;
            BOOL retry;
            BOOL triedFinalizingOpenStatements = NO;

            do {
                retry   = NO;
                rc      = sqlite3_close(_db);
                if (SQLITE_BUSY == rc || SQLITE_LOCKED == rc) {
                    if (!triedFinalizingOpenStatements) {
                        triedFinalizingOpenStatements = YES;
                        sqlite3_stmt *pStmt;
                        while ((pStmt = sqlite3_next_stmt(_db, nil)) !=0) {
                            NSLog(@"Closing leaked statement");
                            sqlite3_finalize(pStmt);
                            pStmt = 0x00;
                            retry = YES;
                        }
                    }
                }
                else if (SQLITE_OK != rc) {
                    NSLog(@"error closing!: %d", rc);
                }
            }
            while (retry);

            _db = nil;
            _isOpen = false;

            return YES;*/
        return true
    }

    /** Test to see if we have a good connection to the database.

     This will confirm whether:

     - is database open

     - if open, it will try a simple @c SELECT statement and confirm that it succeeds.

     @return @c YES if everything succeeds, @c NO on failure.
     */
    var goodConnection: Bool {
        return true
    }


    ///----------------------
    /// @name Perform updates
    ///----------------------

    /** Execute single update statement

    This method executes a single SQL update statement (i.e. any SQL that does not return results, such as @c UPDATE , @c INSERT , or @c DELETE . This method employs [`sqlite3_prepare_v2`](https://sqlite.org/c3ref/prepare.html) and [`sqlite3_bind`](https://sqlite.org/c3ref/bind_blob.html) binding any `?` placeholders in the SQL with the optional list of parameters.

    The optional values provided to this method should be objects (e.g. @c NSString , @c NSNumber , @c NSNull , @c NSDate , and @c NSData  objects), not fundamental data types (e.g. @c int , @c long , @c NSInteger , etc.). This method automatically handles the aforementioned object types, and all other object types will be interpreted as text values using the object's @c description  method.

    @param sql The SQL to be performed, with optional `?` placeholders.

    @param arguments A @c NSArray  of objects to be used when binding values to the `?` placeholders in the SQL statement.

    @return @c YES upon success; @c NO upon failure. If failed, you can call @c lastError , @c lastErrorCode , or @c lastErrorMessage  for diagnostic information regarding the failure.
    beginTransaction
    @see executeUpdate:values:error:
    @see lastError
    @see lastErrorCode
    @see lastErrorMessage
    */
    public func execute (update sql: String, arguments: [Any]? = nil) throws {

    }

    /** Execute single update statement

    This method executes a single SQL update statement (i.e. any SQL that does not return results, such as @c UPDATE , @c INSERT , or @c DELETE . This method employs [`sqlite3_prepare_v2`](https://sqlite.org/c3ref/prepare.html) and [`sqlite_step`](https://sqlite.org/c3ref/step.html) to perform the update. Unlike the other @c executeUpdate methods, this uses printf-style formatters (e.g. `%s`, `%d`, etc.) to build the SQL.

    The optional values provided to this method should be objects (e.g. @c NSString , @c NSNumber , @c NSNull , @c NSDate , and @c NSData  objects), not fundamental data types (e.g. @c int , @c long , @c NSInteger , etc.). This method automatically handles the aforementioned object types, and all other object types will be interpreted as text values using the object's @c description  method.

    @param sql The SQL to be performed, with optional `?` placeholders.

    @param arguments A @c NSDictionary of objects keyed by column names that will be used when binding values to the `?` placeholders in the SQL statement.

    @return @c YES upon success; @c NO upon failure. If failed, you can call @c lastError , @c lastErrorCode , or @c lastErrorMessage  for diagnostic information regarding the failure.

    @see lastError
    @see lastErrorCode
    @see lastErrorMessage
    */
    public func execute (update sql: String, parameterDictionary: [String: Any]) throws {

    }

    /** Execute multiple SQL statements

    This executes a series of SQL statements that are combined in a single string (e.g. the SQL generated by the `sqlite3` command line `.dump` command). This accepts no value parameters, but rather simply expects a single string with multiple SQL statements, each terminated with a semicolon. This uses @c sqlite3_exec .

    @param  sql  The SQL to be performed

    @return      @c YES upon success; @c NO upon failure. If failed, you can call @c lastError , @c lastErrorCode , or @c lastErrorMessage  for diagnostic information regarding the failure.

    @see executeStatements:withResultBlock:
    @see [sqlite3_exec()](https://sqlite.org/c3ref/exec.html)

    */

    public func execute (statements sql: String) throws {

    }

    /** Execute multiple SQL statements with callback handler

    This executes a series of SQL statements that are combined in a single string (e.g. the SQL generated by the `sqlite3` command line `.dump` command). This accepts no value parameters, but rather simply expects a single string with multiple SQL statements, each terminated with a semicolon. This uses `sqlite3_exec`.

    @param sql       The SQL to be performed.
    @param block     A block that will be called for any result sets returned by any SQL statements.
                  Note, if you supply this block, it must return integer value, zero upon success (this would be a good opportunity to use @c SQLITE_OK ),
                  non-zero value upon failure (which will stop the bulk execution of the SQL).  If a statement returns values, the block will be called with the results from the query in NSDictionary *resultsDictionary.
                  This may be @c nil  if you don't care to receive any results.

    @return          @c YES upon success; @c NO upon failure. If failed, you can call @c lastError ,
                  @c lastErrorCode , or @c lastErrorMessage  for diagnostic information regarding the failure.

    @see executeStatements:
    @see [sqlite3_exec()](https://sqlite.org/c3ref/exec.html)

    */
    public func execute (statements sql: String, resultBlock: ([String: Any]) -> ()) throws {

    }

    /** Last insert rowid

    Each entry in an SQLite table has a unique 64-bit signed integer key called the "rowid". The rowid is always available as an undeclared column named `ROWID`, `OID`, or `_ROWID_` as long as those names are not also used by explicitly declared columns. If the table has a column of type `INTEGER PRIMARY KEY` then that column is another alias for the rowid.

    This routine returns the rowid of the most recent successful @c INSERT  into the database from the database connection in the first argument. As of SQLite version 3.7.7, this routines records the last insert rowid of both ordinary tables and virtual tables. If no successful @c INSERT statements have ever occurred on that database connection, zero is returned.

    @return The rowid of the last inserted row.

    @see [sqlite3_last_insert_rowid()](https://sqlite.org/c3ref/last_insert_rowid.html)

    */
    private (set) public var lastInsertRowId: Int64 = 0

    /** The number of rows changed by prior SQL statement.

    This function returns the number of database rows that were changed or inserted or deleted by the most recently completed SQL statement on the database connection specified by the first parameter. Only changes that are directly specified by the @c INSERT , @c UPDATE , or @c DELETE statement are counted.

    @return The number of rows changed by prior SQL statement.

    @see [sqlite3_changes()](https://sqlite.org/c3ref/changes.html)

    */
    private (set) public var changes: Int = 0


    ///-------------------------
    /// @name Retrieving results
    ///-------------------------

    /** Execute select statement

    Executing queries returns an @c FMResultSet  object if successful, and @c nil  upon failure.  Like executing updates, there is a variant that accepts an `NSError **` parameter.  Otherwise you should use the @c lastErrorMessage  and @c lastErrorMessage  methods to determine why a query failed.

    In order to iterate through the results of your query, you use a `while()` loop.  You also need to "step" (via `<[FMResultSet next]>`) from one record to the other.

    This method employs [`sqlite3_bind`](https://sqlite.org/c3ref/bind_blob.html) for any optional value parameters. This  properly escapes any characters that need escape sequences (e.g. quotation marks), which eliminates simple SQL errors as well as protects against SQL injection attacks. This method natively handles @c NSString , @c NSNumber , @c NSNull , @c NSDate , and @c NSData  objects. All other object types will be interpreted as text values using the object's @c description  method.

    @param sql The SELECT statement to be performed, with optional `?` placeholders, followed by optional parameters to bind to `?` placeholders in the SQL statement. These should be Objective-C objects (e.g. @c NSString , @c NSNumber , etc.), not fundamental C data types (e.g. @c int , etc.).

    @return A @c FMResultSet  for the result set upon success; @c nil  upon failure. If failed, you can call @c lastError , @c lastErrorCode , or @c lastErrorMessage  for diagnostic information regarding the failure.

    @see FMResultSet
    @see [`FMResultSet next`](<[FMResultSet next]>)
    @see [`sqlite3_bind`](https://sqlite.org/c3ref/bind_blob.html)

    @note You cannot use this method from Swift due to incompatibilities between Swift and Objective-C variadic implementations. Consider using `<executeQuery:values:>` instead.
    */
    public func execute (query sql: String, arguments: [Any]? = nil) throws -> FMResultSet {
        return FMResultSet()
    }


    /** Execute select statement

    Executing queries returns an @c FMResultSet  object if successful, and @c nil  upon failure.  Like executing updates, there is a variant that accepts an `NSError **` parameter.  Otherwise you should use the @c lastErrorMessage  and @c lastErrorMessage  methods to determine why a query failed.

    In order to iterate through the results of your query, you use a `while()` loop.  You also need to "step" (via `<[FMResultSet next]>`) from one record to the other.

    @param sql The SELECT statement to be performed, with optional `?` placeholders.

    @param arguments A @c NSDictionary of objects keyed by column names that will be used when binding values to the `?` placeholders in the SQL statement.

    @return A @c FMResultSet  for the result set upon success; @c nil  upon failure. If failed, you can call @c lastError , @c lastErrorCode , or @c lastErrorMessage  for diagnostic information regarding the failure.

    @see FMResultSet
    @see [`FMResultSet next`](<[FMResultSet next]>)
    */
    public func execute (query sql: String, parameterDictionary: [String: Any]) throws -> FMResultSet {
        return FMResultSet()
    }

    /// Prepare SQL statement.
    ///
    /// @param sql SQL statement to prepare, generally with `?` placeholders.
    public func prepare (sql: String) -> FMResultSet {
        return FMResultSet()
    }

    ///-------------------
    /// @name Transactions
    ///-------------------

    /** Begin a transaction

    @return @c YES on success; @c NO on failure. If failed, you can call @c lastError , @c lastErrorCode , or @c lastErrorMessage  for diagnostic information regarding the failure.

    @see commit
    @see rollback
    @see beginDeferredTransaction
    @see isInTransaction

    @warning    Unlike SQLite's `BEGIN TRANSACTION`, this method currently performs
             an exclusive transaction, not a deferred transaction. This behavior
             is likely to change in future versions of FMDB, whereby this method
             will likely eventually adopt standard SQLite behavior and perform
             deferred transactions. If you really need exclusive tranaction, it is
             recommended that you use @c beginExclusiveTransaction, instead, not
             only to make your intent explicit, but also to future-proof your code.

    */
    public func beginTransaction () throws {

    }

    /** Begin a deferred transaction

    @return @c YES on success; @c NO on failure. If failed, you can call @c lastError , @c lastErrorCode , or @c lastErrorMessage  for diagnostic information regarding the failure.

    @see commit
    @see rollback
    @see beginTransaction
    @see isInTransaction
    */
    public func beginDeferredTransaction () throws {

    }

    /** Begin an immediate transaction

    @return @c YES on success; @c NO on failure. If failed, you can call @c lastError , @c lastErrorCode , or @c lastErrorMessage  for diagnostic information regarding the failure.

    @see commit
    @see rollback
    @see beginTransaction
    @see isInTransaction
    */

    public func beginImmediateTransaction () throws {}

    /** Begin an exclusive transaction

    @return @c YES on success; @c NO on failure. If failed, you can call @c lastError , @c lastErrorCode , or @c lastErrorMessage  for diagnostic information regarding the failure.

    @see commit
    @see rollback
    @see beginTransaction
    @see isInTransaction
    */
    public func beginExclusiveTransaction () throws {}

    /** Commit a transaction

    Commit a transaction that was initiated with either `<beginTransaction>` or with `<beginDeferredTransaction>`.

    @return @c YES on success; @c NO on failure. If failed, you can call @c lastError , @c lastErrorCode , or @c lastErrorMessage  for diagnostic information regarding the failure.

    @see beginTransaction
    @see beginDeferredTransaction
    @see rollback
    @see isInTransaction
    */
    public func commit () throws {}

    /** Rollback a transaction

    Rollback a transaction that was initiated with either `<beginTransaction>` or with `<beginDeferredTransaction>`.

    @return @c YES on success; @c NO on failure. If failed, you can call @c lastError , @c lastErrorCode , or @c lastErrorMessage  for diagnostic information regarding the failure.

    @see beginTransaction
    @see beginDeferredTransaction
    @see commit
    @see isInTransaction
    */
    public func rollback () throws {}

    /** Identify whether currently in a transaction or not

    @see beginTransaction
    @see beginDeferredTransaction
    @see commit
    @see rollback
    */
    private (set) public var isInTransaction = false



    ///----------------------------------------
    /// @name Cached statements and result sets
    ///----------------------------------------

    /** Clear cached statements */

        // MARK: Cached statements
    public func clearCachedStatements () {
    // FIXME: needs work
            for statements in cachedStatements.enumerated() {
                for statement in statements.element.value {
                    (statement as! FMStatement).close()
                }
            }
            cachedStatements.removeAll()
        }

        /*
        - (FMStatement*)cachedStatementForQuery:(NSString*)query {

            NSMutableSet* statements = [_cachedStatements objectForKey:query];

            return [[statements objectsPassingTest:^BOOL(FMStatement* statement, BOOL *stop) {

                *stop = ![statement inUse];
                return *stop;

            }] anyObject];
        }


        - (void)setCachedStatement:(FMStatement*)statement forQuery:(NSString*)query {
            NSParameterAssert(query);
            if (!query) {
                NSLog(@"API misuse, -[FMDatabase setCachedStatement:forQuery:] query must not be nil");
                return;
            }

            query = [query copy]; // in case we got handed in a mutable string...
            [statement setQuery:query];

            NSMutableSet* statements = [_cachedStatements objectForKey:query];
            if (!statements) {
                statements = [NSMutableSet set];
            }

            [statements addObject:statement];

            [_cachedStatements setObject:statements forKey:query];

            FMDBRelease(query);
        }
        */
    /** Close all open result sets */
    private func closeOpenResultSets () {
// FIXME: needs work
        /*for
        //Copy the set so we don't get mutation errors
        NSSet *openSetCopy = FMDBReturnAutoreleased([_openResultSets copy]);

        for (NSValue *rsInWrappedInATastyValueMeal in openSetCopy) {
            FMResultSet *rs = (FMResultSet *)[rsInWrappedInATastyValueMeal pointerValue];

            [rs setParentDB:nil];
            [rs close];

            [_openResultSets removeObject:rsInWrappedInATastyValueMeal];
        }*/
    }

    /*
    - (void)resultSetDidClose:(FMResultSet *)resultSet {
        NSValue *setValue = [NSValue valueWithNonretainedObject:resultSet];

        [_openResultSets removeObject:setValue];
    }
    */

    /** Whether database has any open result sets

    @return @c YES if there are open result sets; @c NO if not.
    */
    var hasOpenResultSets: Bool {
        return !_openResultSets.isEmpty
    }


    /** Whether should cache statements or not */
    public var shouldCacheStatements: Bool = false

    /** Interupt pending database operation

    This method causes any pending database operation to abort and return at its earliest opportunity

    @return @c YES on success; @c NO on failure. If failed, you can call @c lastError , @c lastErrorCode , or @c lastErrorMessage  for diagnostic information regarding the failure.

    */
    public func interrupt () throws {}

    ///-------------------------
    /// @name Encryption methods
    ///-------------------------

    /** Set encryption key.

     @param key The key to be used.

     @return @c YES if success, @c NO on error.

     @see https://www.zetetic.net/sqlcipher/

     @warning You need to have purchased the sqlite encryption extensions for this method to work.
     */
     public func set (key: String) throws {}

    /** Reset encryption key

     @param key The key to be used.

     @return @c YES if success, @c NO on error.

     @see https://www.zetetic.net/sqlcipher/

     @warning You need to have purchased the sqlite encryption extensions for this method to work.
     */
    public func re (key: String) throws {}

    /** Set encryption key using `keyData`.

    @param keyData The @c NSData  to be used.

    @return @c YES if success, @c NO on error.

    @see https://www.zetetic.net/sqlcipher/

    @warning You need to have purchased the sqlite encryption extensions for this method to work.
    */

    public func set (key: Data) throws {}

    /** Reset encryption key using `keyData`.

    @param keyData The @c NSData  to be used.

    @return @c YES if success, @c NO on error.

    @see https://www.zetetic.net/sqlcipher/

    @warning You need to have purchased the sqlite encryption extensions for this method to work.
    */
    public func re (key: Data) throws {}

     /** The file URL of the database file. */
    public let databaseURL: URL?

    /** The underlying SQLite handle .

     @return The `sqlite3` pointer.

    */
    public var sqliteHandle: OpaquePointer? {
        return _db
    }


    ///-----------------------------
    /// @name Retrieving error codes
    ///-----------------------------

    /** Last error message

     Returns the English-language text that describes the most recent failed SQLite API call associated with a database connection. If a prior API call failed but the most recent API call succeeded, this return value is undefined.

     @return @c NSString  of the last error message.

     @see [sqlite3_errmsg()](https://sqlite.org/c3ref/errcode.html)
     @see lastErrorCode
     @see lastError

    */
    private (set) public var lastErrorMessage: String?

    /** Last error code

    Returns the numeric result code or extended result code for the most recent failed SQLite API call associated with a database connection. If a prior API call failed but the most recent API call succeeded, this return value is undefined.

    @return Integer value of the last error code.

    @see [sqlite3_errcode()](https://sqlite.org/c3ref/errcode.html)
    @see lastErrorMessage
    @see lastError

    */
    private (set) public var lastErrorCode: Int32?


    /** Last extended error code

    Returns the numeric extended result code for the most recent failed SQLite API call associated with a database connection. If a prior API call failed but the most recent API call succeeded, this return value is undefined.

    @return Integer value of the last extended error code.

    @see [sqlite3_errcode()](https://sqlite.org/c3ref/errcode.html)
    @see [2. Primary Result Codes versus Extended Result Codes](https://sqlite.org/rescode.html#primary_result_codes_versus_extended_result_codes)
    @see [5. Extended Result Code List](https://sqlite.org/rescode.html#extrc)
    @see lastErrorMessage
    @see lastError

    */
    private (set) public var varlastExtendedErrorCode: Int32?

    /** Had error

    @return @c YES if there was an error, @c NO if no error.

    @see lastError
    @see lastErrorCode
    @see lastErrorMessage

    */
    private (set) public var hadError = false

    /** Last error

    @return @c NSError  representing the last error.

    @see lastErrorCode
    @see lastErrorMessage

    */

    private (set) public var lastError: Error?

    public var maxBusyRetryTimeInterval: TimeInterval = 2 {
        didSet {
            setMaxBusyRetryTimeInterval()
        }
    }

    private func setMaxBusyRetryTimeInterval () {

        if _db == nil {
            return
        }

        if maxBusyRetryTimeInterval > 0 {
            sqlite3_busy_handler(_db, databaseBusyHandler, Unmanaged.passUnretained(self).toOpaque())
        }
        else {
            // turn it off otherwise
            sqlite3_busy_handler(_db, nil, nil)
        }
    }

    ///------------------
    /// @name Save points
    ///------------------

    /** Start save point

    @param name Name of save point.

    @param outErr A @c NSError  object to receive any error object (if any).

    @return @c YES on success; @c NO on failure. If failed, you can call @c lastError , @c lastErrorCode , or @c lastErrorMessage  for diagnostic information regarding the failure.

    @see releaseSavePointWithName:error:
    @see rollbackToSavePointWithName:error:
    */
    public func start (savePoint name: String) throws {

    }

    /** Release save point

    @param name Name of save point.

    @param outErr A @c NSError  object to receive any error object (if any).

    @return @c YES on success; @c NO on failure. If failed, you can call @c lastError , @c lastErrorCode , or @c lastErrorMessage  for diagnostic information regarding the failure.

    @see startSavePointWithName:error:
    @see rollbackToSavePointWithName:error:

    */
    public func release (savePoint name: String) throws {

    }

    /** Roll back to save point

    @param name Name of save point.
    @param outErr A @c NSError  object to receive any error object (if any).

    @return @c YES on success; @c NO on failure. If failed, you can call @c lastError , @c lastErrorCode , or @c lastErrorMessage  for diagnostic information regarding the failure.

    @see startSavePointWithName:error:
    @see releaseSavePointWithName:error:

    */
    public func rollbackTo (savePoint name: String) throws {

    }

    /** Start save point

    @param block Block of code to perform from within save point.

    @return The NSError corresponding to the error, if any. If no error, returns @c nil .

    @see startSavePointWithName:error:
    @see releaseSavePointWithName:error:
    @see rollbackToSavePointWithName:error:

    */
    public func inSavePoint (block: ((Bool) -> Void)) throws {

    }


    ///-----------------
    /// @name Checkpoint
    ///-----------------

    /** Performs a WAL checkpoint

    @param checkpointMode The checkpoint mode for sqlite3_wal_checkpoint_v2
    @param name The db name for sqlite3_wal_checkpoint_v2
    @param error The NSError corresponding to the error, if any.
    @param logFrameCount If not @c NULL , then this is set to the total number of frames in the log file or to -1 if the checkpoint could not run because of an error or because the database is not in WAL mode.
    @param checkpointCount If not @c NULL , then this is set to the total number of checkpointed frames in the log file (including any that were already checkpointed before the function was called) or to -1 if the checkpoint could not run due to an error or because the database is not in WAL mode.
    @return @c YES on success, otherwise @c NO .
    */
    public func checkpoint (mode: FMDBCheckpointMode, name: String? = nil,
            logFrameCount: inout Int?, checkpointCount: inout Int?) throws {

    }


    ///----------------------------
    /// @name SQLite library status
    ///----------------------------

    /** Test to see if the library is threadsafe

    @return @c NO if and only if SQLite was compiled with mutexing code omitted due to the @c SQLITE_THREADSAFE compile-time option being set to 0.

    @see [sqlite3_threadsafe()](https://sqlite.org/c3ref/threadsafe.html)
    */
    public var isSQLiteThreadSafe: Bool {
        return sqlite3_threadsafe() != 0
    }

    /** Examine/set limits

     @param type The type of limit. See https://sqlite.org/c3ref/c_limit_attached.html
     @param newLimit The new limit value. Use -1 if you don't want to change the limit, but rather only want to check it.

     @return Regardless, returns previous value.

     @see [sqlite3_limit()](https://sqlite.org/c3ref/limit.html)
    */
    public func limit (for type: Int32, value newLimit: Int32 = -1) -> Int {
        return Int(sqlite3_limit(_db, type, newLimit))
    }

    /** Run-time library version numbers

    @return The sqlite library version string.

    @see [sqlite3_libversion()](https://sqlite.org/c3ref/libversion.html)
    */
    public var sqliteLibVersion: String {
        return String(cString: sqlite3_libversion(), encoding: .utf8) ?? "Unknown"
    }

    /// The FMDB version number as a string in the form of @c "2.7.7" .
    ///
    /// If you want to compare version number strings, you can use NSNumericSearch option:
    ///
    /// @code
    /// NSComparisonResult result = [[FMDatabase FMDBUserVersion] compare:@"2.11.0" options:NSNumericSearch];
    /// @endcode
    ///
    /// @returns The version number string.
    public let fmdbUserVersion = "2.7.7"

    ///------------------------
    /// @name Make SQL function
    ///------------------------

    /** Adds SQL functions or aggregates or to redefine the behavior of existing SQL functions or aggregates.

    For example:

    @code
    [db makeFunctionNamed:@"RemoveDiacritics" arguments:1 block:^(void *context, int argc, void **argv) {
    SqliteValueType type = [self.db valueType:argv[0]];
    if (type == SqliteValueTypeNull) {
        [self.db resultNullInContext:context];
         return;
    }
    if (type != SqliteValueTypeText) {
        [self.db resultError:@"Expected text" context:context];
        return;
    }
    NSString *string = [self.db valueString:argv[0]];
    NSString *result = [string stringByFoldingWithOptions:NSDiacriticInsensitiveSearch locale:nil];
    [self.db resultString:result context:context];
    }];

    FMResultSet *rs = [db executeQuery:@"SELECT * FROM employees WHERE RemoveDiacritics(first_name) LIKE 'jose'"];
    NSAssert(rs, @"Error %@", [db lastErrorMessage]);
    @endcode

    @param name Name of function.

    @param arguments Maximum number of parameters.

    @param block The block of code for the function.

    @see [sqlite3_create_function()](https://sqlite.org/c3ref/create_function.html)
    */
/*
- (void)makeFunctionNamed:(NSString *)name arguments:(int)arguments block:(void (^)(void *context, int argc, void * _Nonnull * _Nonnull argv))block;

- (void)makeFunctionNamed:(NSString *)name maximumArguments:(int)count withBlock:(void (^)(void *context, int argc, void * _Nonnull * _Nonnull argv))block __deprecated_msg("Use makeFunctionNamed:arguments:block:");

typedef NS_ENUM(int, SqliteValueType) {
    SqliteValueTypeInteger = 1,
    SqliteValueTypeFloat   = 2,
    SqliteValueTypeText    = 3,
    SqliteValueTypeBlob    = 4,
    SqliteValueTypeNull    = 5
};

- (SqliteValueType)valueType:(void *)argv;

/**
 Get integer value of parameter in custom function.

 @param value The argument whose value to return.
 @return The integer value.

 @see makeFunctionNamed:arguments:block:
 */
- (int)valueInt:(void *)value;

/**
 Get long value of parameter in custom function.

 @param value The argument whose value to return.
 @return The long value.

 @see makeFunctionNamed:arguments:block:
 */
- (long long)valueLong:(void *)value;

/**
 Get double value of parameter in custom function.

 @param value The argument whose value to return.
 @return The double value.

 @see makeFunctionNamed:arguments:block:
 */
- (double)valueDouble:(void *)value;

/**
 Get @c NSData  value of parameter in custom function.

 @param value The argument whose value to return.
 @return The data object.

 @see makeFunctionNamed:arguments:block:
 */
- (NSData * _Nullable)valueData:(void *)value;

/**
 Get string value of parameter in custom function.

 @param value The argument whose value to return.
 @return The string value.

 @see makeFunctionNamed:arguments:block:
 */
- (NSString * _Nullable)valueString:(void *)value;

/**
 Return null value from custom function.

 @param context The context to which the null value will be returned.

 @see makeFunctionNamed:arguments:block:
 */
- (void)resultNullInContext:(void *)context NS_SWIFT_NAME(resultNull(context:));

/**
 Return integer value from custom function.

 @param value The integer value to be returned.
 @param context The context to which the value will be returned.

 @see makeFunctionNamed:arguments:block:
 */
- (void)resultInt:(int) value context:(void *)context;

/**
 Return long value from custom function.

 @param value The long value to be returned.
 @param context The context to which the value will be returned.

 @see makeFunctionNamed:arguments:block:
 */
- (void)resultLong:(long long)value context:(void *)context;

/**
 Return double value from custom function.

 @param value The double value to be returned.
 @param context The context to which the value will be returned.

 @see makeFunctionNamed:arguments:block:
 */
- (void)resultDouble:(double)value context:(void *)context;

/**
 Return @c NSData  object from custom function.

 @param data The @c NSData  object to be returned.
 @param context The context to which the value will be returned.

 @see makeFunctionNamed:arguments:block:
 */
- (void)resultData:(NSData *)data context:(void *)context;

/**
 Return string value from custom function.

 @param value The string value to be returned.
 @param context The context to which the value will be returned.

 @see makeFunctionNamed:arguments:block:
 */
- (void)resultString:(NSString *)value context:(void *)context;

/**
 Return error string from custom function.

 @param error The error string to be returned.
 @param context The context to which the error will be returned.

 @see makeFunctionNamed:arguments:block:
 */
- (void)resultError:(NSString *)error context:(void *)context;

/**
 Return error code from custom function.

 @param errorCode The integer error code to be returned.
 @param context The context to which the error will be returned.

 @see makeFunctionNamed:arguments:block:
 */
- (void)resultErrorCode:(int)errorCode context:(void *)context;

/**
 Report memory error in custom function.

 @param context The context to which the error will be returned.

 @see makeFunctionNamed:arguments:block:
 */
- (void)resultErrorNoMemoryInContext:(void *)context NS_SWIFT_NAME(resultErrorNoMemory(context:));

/**
 Report that string or BLOB is too long to represent in custom function.

 @param context The context to which the error will be returned.

 @see makeFunctionNamed:arguments:block:
 */
- (void)resultErrorTooBigInContext:(void *)context NS_SWIFT_NAME(resultErrorTooBig(context:));

///---------------------
/// @name Date formatter
///---------------------

/** Generate an @c NSDateFormatter  that won't be broken by permutations of timezones or locales.

 Use this method to generate values to set the dateFormat property.

 Example:

@code
myDB.dateFormat = [FMDatabase storeableDateFormat:@"yyyy-MM-dd HH:mm:ss"];
@endcode

 @param format A valid NSDateFormatter format string.

 @return A @c NSDateFormatter  that can be used for converting dates to strings and vice versa.

 @see hasDateFormatter
 @see setDateFormat:
 @see dateFromString:
 @see stringFromDate:
 @see storeableDateFormat:

 @warning Note that @c NSDateFormatter  is not thread-safe, so the formatter generated by this method should be assigned to only one FMDB instance and should not be used for other purposes.

 */

+ (NSDateFormatter *)storeableDateFormat:(NSString *)format;

/** Test whether the database has a date formatter assigned.

 @return @c YES if there is a date formatter; @c NO if not.

 @see hasDateFormatter
 @see setDateFormat:
 @see dateFromString:
 @see stringFromDate:
 @see storeableDateFormat:
 */

- (BOOL)hasDateFormatter;

/** Set to a date formatter to use string dates with sqlite instead of the default UNIX timestamps.

 @param format Set to nil to use UNIX timestamps. Defaults to nil. Should be set using a formatter generated using @c FMDatabase:storeableDateFormat .

 @see hasDateFormatter
 @see setDateFormat:
 @see dateFromString:
 @see stringFromDate:
 @see storeableDateFormat:

 @warning Note there is no direct getter for the @c NSDateFormatter , and you should not use the formatter you pass to FMDB for other purposes, as @c NSDateFormatter  is not thread-safe.
 */

- (void)setDateFormat:(NSDateFormatter * _Nullable)format;

/** Convert the supplied NSString to NSDate, using the current database formatter.

 @param s @c NSString  to convert to @c NSDate .

 @return The @c NSDate  object; or @c nil  if no formatter is set.

 @see hasDateFormatter
 @see setDateFormat:
 @see dateFromString:
 @see stringFromDate:
 @see storeableDateFormat:
 */

- (NSDate * _Nullable)dateFromString:(NSString *)s;

/** Convert the supplied NSDate to NSString, using the current database formatter.

 @param date @c NSDate  of date to convert to @c NSString .

 @return The @c NSString  representation of the date; @c nil  if no formatter is set.

 @see hasDateFormatter
 @see setDateFormat:
 @see dateFromString:
 @see stringFromDate:
 @see storeableDateFormat:
 */

- (NSString * _Nullable)stringFromDate:(NSDate *)date;

@end


/** Objective-C wrapper for @c sqlite3_stmt

 This is a wrapper for a SQLite @c sqlite3_stmt . Generally when using FMDB you will not need to interact directly with `FMStatement`, but rather with @c FMDatabase  and @c FMResultSet  only.

 See also

 - @c FMDatabase
 - @c FMResultSet
 - [@c sqlite3_stmt ](https://sqlite.org/c3ref/stmt.html)
 */

@interface FMStatement : NSObject {
    void *_statement;
    NSString *_query;
    long _useCount;
    BOOL _inUse;
}

///-----------------
/// @name Properties
///-----------------

/** Usage count */

@property (atomic, assign) long useCount;

/** SQL statement */

@property (atomic, retain) NSString *query;

/** SQLite sqlite3_stmt

 @see [@c sqlite3_stmt ](https://sqlite.org/c3ref/stmt.html)
 */

@property (atomic, assign) void *statement;

/** Indication of whether the statement is in use */

@property (atomic, assign) BOOL inUse;

///----------------------------
/// @name Closing and Resetting
///----------------------------

/** Close statement */

- (void)close;

/** Reset statement */

- (void)reset;

@end

#pragma clang diagnostic pop

NS_ASSUME_NONNULL_END

*/


/*


// we no longer make busyRetryTimeout public
// but for folks who don't bother noticing that the interface to FMDatabase changed,
// we'll still implement the method so they don't get suprise crashes
- (int)busyRetryTimeout {
    NSLog(@"%s:%d", __FUNCTION__, __LINE__);
    NSLog(@"FMDB: busyRetryTimeout no longer works, please use maxBusyRetryTimeInterval");
    return -1;
}

- (void)setBusyRetryTimeout:(int)i {
#pragma unused(i)
    NSLog(@"%s:%d", __FUNCTION__, __LINE__);
    NSLog(@"FMDB: setBusyRetryTimeout does nothing, please use setMaxBusyRetryTimeInterval:");
}




#pragma mark Key routines

- (BOOL)rekey:(NSString*)key {
    NSData *keyData = [NSData dataWithBytes:(void *)[key UTF8String] length:(NSUInteger)strlen([key UTF8String])];

    return [self rekeyWithData:keyData];
}

- (BOOL)rekeyWithData:(NSData *)keyData {
#ifdef SQLITE_HAS_CODEC
    if (!keyData) {
        return NO;
    }

    int rc = sqlite3_rekey(_db, [keyData bytes], (int)[keyData length]);

    if (rc != SQLITE_OK) {
        NSLog(@"error on rekey: %d", rc);
        NSLog(@"%@", [self lastErrorMessage]);
    }

    return (rc == SQLITE_OK);
#else
#pragma unused(keyData)
    return NO;
#endif
}

- (BOOL)setKey:(NSString*)key {
    NSData *keyData = [NSData dataWithBytes:[key UTF8String] length:(NSUInteger)strlen([key UTF8String])];

    return [self setKeyWithData:keyData];
}

- (BOOL)setKeyWithData:(NSData *)keyData {
#ifdef SQLITE_HAS_CODEC
    if (!keyData) {
        return NO;
    }

    int rc = sqlite3_key(_db, [keyData bytes], (int)[keyData length]);

    return (rc == SQLITE_OK);
#else
#pragma unused(keyData)
    return NO;
#endif
}

#pragma mark Date routines

+ (NSDateFormatter *)storeableDateFormat:(NSString *)format {

    NSDateFormatter *result = FMDBReturnAutoreleased([[NSDateFormatter alloc] init]);
    result.dateFormat = format;
    result.timeZone = [NSTimeZone timeZoneForSecondsFromGMT:0];
    result.locale = FMDBReturnAutoreleased([[NSLocale alloc] initWithLocaleIdentifier:@"en_US"]);
    return result;
}


- (BOOL)hasDateFormatter {
    return _dateFormat != nil;
}

- (void)setDateFormat:(NSDateFormatter *)format {
    FMDBAutorelease(_dateFormat);
    _dateFormat = FMDBReturnRetained(format);
}

- (NSDate *)dateFromString:(NSString *)s {
    return [_dateFormat dateFromString:s];
}

- (NSString *)stringFromDate:(NSDate *)date {
    return [_dateFormat stringFromDate:date];
}

#pragma mark State of database

- (BOOL)goodConnection {

    if (!_isOpen) {
        return NO;
    }

#ifdef SQLCIPHER_CRYPTO
    // Starting with Xcode8 / iOS 10 we check to make sure we really are linked with
    // SQLCipher because there is no longer a linker error if we accidently link
    // with unencrypted sqlite library.
    //
    // https://discuss.zetetic.net/t/important-advisory-sqlcipher-with-xcode-8-and-new-sdks/1688

    FMResultSet *rs = [self executeQuery:@"PRAGMA cipher_version"];

    if ([rs next]) {
        NSLog(@"SQLCipher version: %@", rs.resultDictionary[@"cipher_version"]);

        [rs close];
        return YES;
    }
#else
    FMResultSet *rs = [self executeQuery:@"select name from sqlite_master where type='table'"];

    if (rs) {
        [rs close];
        return YES;
    }
#endif

    return NO;
}

- (void)warnInUse {
    NSLog(@"The FMDatabase %@ is currently in use.", self);

#ifndef NS_BLOCK_ASSERTIONS
    if (_crashOnErrors) {
        NSAssert(false, @"The FMDatabase %@ is currently in use.", self);
        abort();
    }
#endif
}

- (BOOL)databaseExists {

    if (!_isOpen) {

        NSLog(@"The FMDatabase %@ is not open.", self);

#ifndef NS_BLOCK_ASSERTIONS
        if (_crashOnErrors) {
            NSAssert(false, @"The FMDatabase %@ is not open.", self);
            abort();
        }
#endif

        return NO;
    }

    return YES;
}

#pragma mark Error routines

- (NSString *)lastErrorMessage {
    return [NSString stringWithUTF8String:sqlite3_errmsg(_db)];
}

- (BOOL)hadError {
    int lastErrCode = [self lastErrorCode];

    return (lastErrCode > SQLITE_OK && lastErrCode < SQLITE_ROW);
}

- (int)lastErrorCode {
    return sqlite3_errcode(_db);
}

- (int)lastExtendedErrorCode {
    return sqlite3_extended_errcode(_db);
}

- (NSError*)errorWithMessage:(NSString *)message {
    NSDictionary* errorMessage = [NSDictionary dictionaryWithObject:message forKey:NSLocalizedDescriptionKey];

    return [NSError errorWithDomain:@"FMDatabase" code:sqlite3_errcode(_db) userInfo:errorMessage];
}

- (NSError*)lastError {
    return [self errorWithMessage:[self lastErrorMessage]];
}

#pragma mark Update information routines

- (sqlite_int64)lastInsertRowId {

    if (_isExecutingStatement) {
        [self warnInUse];
        return NO;
    }

    _isExecutingStatement = YES;

    sqlite_int64 ret = sqlite3_last_insert_rowid(_db);

    _isExecutingStatement = NO;

    return ret;
}

- (int)changes {
    if (_isExecutingStatement) {
        [self warnInUse];
        return 0;
    }

    _isExecutingStatement = YES;

    int ret = sqlite3_changes(_db);

    _isExecutingStatement = NO;

    return ret;
}

#pragma mark SQL manipulation

- (int)bindObject:(id)obj toColumn:(int)idx inStatement:(sqlite3_stmt*)pStmt {

    if ((!obj) || ((NSNull *)obj == [NSNull null])) {
        return sqlite3_bind_null(pStmt, idx);
    }

    // FIXME - someday check the return codes on these binds.
    else if ([obj isKindOfClass:[NSData class]]) {
        const void *bytes = [obj bytes];
        if (!bytes) {
            // it's an empty NSData object, aka [NSData data].
            // Don't pass a NULL pointer, or sqlite will bind a SQL null instead of a blob.
            bytes = "";
        }
        return sqlite3_bind_blob(pStmt, idx, bytes, (int)[obj length], SQLITE_TRANSIENT);
    }
    else if ([obj isKindOfClass:[NSDate class]]) {
        if (self.hasDateFormatter)
            return sqlite3_bind_text(pStmt, idx, [[self stringFromDate:obj] UTF8String], -1, SQLITE_TRANSIENT);
        else
            return sqlite3_bind_double(pStmt, idx, [obj timeIntervalSince1970]);
    }
    else if ([obj isKindOfClass:[NSNumber class]]) {

        if (strcmp([obj objCType], @encode(char)) == 0) {
            return sqlite3_bind_int(pStmt, idx, [obj charValue]);
        }
        else if (strcmp([obj objCType], @encode(unsigned char)) == 0) {
            return sqlite3_bind_int(pStmt, idx, [obj unsignedCharValue]);
        }
        else if (strcmp([obj objCType], @encode(short)) == 0) {
            return sqlite3_bind_int(pStmt, idx, [obj shortValue]);
        }
        else if (strcmp([obj objCType], @encode(unsigned short)) == 0) {
            return sqlite3_bind_int(pStmt, idx, [obj unsignedShortValue]);
        }
        else if (strcmp([obj objCType], @encode(int)) == 0) {
            return sqlite3_bind_int(pStmt, idx, [obj intValue]);
        }
        else if (strcmp([obj objCType], @encode(unsigned int)) == 0) {
            return sqlite3_bind_int64(pStmt, idx, (long long)[obj unsignedIntValue]);
        }
        else if (strcmp([obj objCType], @encode(long)) == 0) {
            return sqlite3_bind_int64(pStmt, idx, [obj longValue]);
        }
        else if (strcmp([obj objCType], @encode(unsigned long)) == 0) {
            return sqlite3_bind_int64(pStmt, idx, (long long)[obj unsignedLongValue]);
        }
        else if (strcmp([obj objCType], @encode(long long)) == 0) {
            return sqlite3_bind_int64(pStmt, idx, [obj longLongValue]);
        }
        else if (strcmp([obj objCType], @encode(unsigned long long)) == 0) {
            return sqlite3_bind_int64(pStmt, idx, (long long)[obj unsignedLongLongValue]);
        }
        else if (strcmp([obj objCType], @encode(float)) == 0) {
            return sqlite3_bind_double(pStmt, idx, [obj floatValue]);
        }
        else if (strcmp([obj objCType], @encode(double)) == 0) {
            return sqlite3_bind_double(pStmt, idx, [obj doubleValue]);
        }
        else if (strcmp([obj objCType], @encode(BOOL)) == 0) {
            return sqlite3_bind_int(pStmt, idx, ([obj boolValue] ? 1 : 0));
        }
        else {
            return sqlite3_bind_text(pStmt, idx, [[obj description] UTF8String], -1, SQLITE_TRANSIENT);
        }
    }

    return sqlite3_bind_text(pStmt, idx, [[obj description] UTF8String], -1, SQLITE_TRANSIENT);
}

- (void)extractSQL:(NSString *)sql argumentsList:(va_list)args intoString:(NSMutableString *)cleanedSQL arguments:(NSMutableArray *)arguments {

    NSUInteger length = [sql length];
    unichar last = '\0';
    for (NSUInteger i = 0; i < length; ++i) {
        id arg = nil;
        unichar current = [sql characterAtIndex:i];
        unichar add = current;
        if (last == '%') {
            switch (current) {
                case '@':
                    arg = va_arg(args, id);
                    break;
                case 'c':
                    // warning: second argument to 'va_arg' is of promotable type 'char'; this va_arg has undefined behavior because arguments will be promoted to 'int'
                    arg = [NSString stringWithFormat:@"%c", va_arg(args, int)];
                    break;
                case 's':
                    arg = [NSString stringWithUTF8String:va_arg(args, char*)];
                    break;
                case 'd':
                case 'D':
                case 'i':
                    arg = [NSNumber numberWithInt:va_arg(args, int)];
                    break;
                case 'u':
                case 'U':
                    arg = [NSNumber numberWithUnsignedInt:va_arg(args, unsigned int)];
                    break;
                case 'h':
                    i++;
                    if (i < length && [sql characterAtIndex:i] == 'i') {
                        //  warning: second argument to 'va_arg' is of promotable type 'short'; this va_arg has undefined behavior because arguments will be promoted to 'int'
                        arg = [NSNumber numberWithShort:(short)(va_arg(args, int))];
                    }
                    else if (i < length && [sql characterAtIndex:i] == 'u') {
                        // warning: second argument to 'va_arg' is of promotable type 'unsigned short'; this va_arg has undefined behavior because arguments will be promoted to 'int'
                        arg = [NSNumber numberWithUnsignedShort:(unsigned short)(va_arg(args, uint))];
                    }
                    else {
                        i--;
                    }
                    break;
                case 'q':
                    i++;
                    if (i < length && [sql characterAtIndex:i] == 'i') {
                        arg = [NSNumber numberWithLongLong:va_arg(args, long long)];
                    }
                    else if (i < length && [sql characterAtIndex:i] == 'u') {
                        arg = [NSNumber numberWithUnsignedLongLong:va_arg(args, unsigned long long)];
                    }
                    else {
                        i--;
                    }
                    break;
                case 'f':
                    arg = [NSNumber numberWithDouble:va_arg(args, double)];
                    break;
                case 'g':
                    // warning: second argument to 'va_arg' is of promotable type 'float'; this va_arg has undefined behavior because arguments will be promoted to 'double'
                    arg = [NSNumber numberWithFloat:(float)(va_arg(args, double))];
                    break;
                case 'l':
                    i++;
                    if (i < length) {
                        unichar next = [sql characterAtIndex:i];
                        if (next == 'l') {
                            i++;
                            if (i < length && [sql characterAtIndex:i] == 'd') {
                                //%lld
                                arg = [NSNumber numberWithLongLong:va_arg(args, long long)];
                            }
                            else if (i < length && [sql characterAtIndex:i] == 'u') {
                                //%llu
                                arg = [NSNumber numberWithUnsignedLongLong:va_arg(args, unsigned long long)];
                            }
                            else {
                                i--;
                            }
                        }
                        else if (next == 'd') {
                            //%ld
                            arg = [NSNumber numberWithLong:va_arg(args, long)];
                        }
                        else if (next == 'u') {
                            //%lu
                            arg = [NSNumber numberWithUnsignedLong:va_arg(args, unsigned long)];
                        }
                        else {
                            i--;
                        }
                    }
                    else {
                        i--;
                    }
                    break;
                default:
                    // something else that we can't interpret. just pass it on through like normal
                    break;
            }
        }
        else if (current == '%') {
            // percent sign; skip this character
            add = '\0';
        }

        if (arg != nil) {
            [cleanedSQL appendString:@"?"];
            [arguments addObject:arg];
        }
        else if (add == (unichar)'@' && last == (unichar) '%') {
            [cleanedSQL appendFormat:@"NULL"];
        }
        else if (add != '\0') {
            [cleanedSQL appendFormat:@"%C", add];
        }
        last = current;
    }
}

#pragma mark Execute queries

- (FMResultSet *)executeQuery:(NSString *)sql withParameterDictionary:(NSDictionary *)arguments {
    return [self executeQuery:sql withArgumentsInArray:nil orDictionary:arguments orVAList:nil shouldBind:true];
}

- (FMResultSet *)executeQuery:(NSString *)sql withArgumentsInArray:(NSArray*)arrayArgs orDictionary:(NSDictionary *)dictionaryArgs orVAList:(va_list)args shouldBind:(BOOL)shouldBind {
    if (![self databaseExists]) {
        return 0x00;
    }

    if (_isExecutingStatement) {
        [self warnInUse];
        return 0x00;
    }

    _isExecutingStatement = YES;

    int rc                  = 0x00;
    sqlite3_stmt *pStmt     = 0x00;
    FMStatement *statement  = 0x00;
    FMResultSet *rs         = 0x00;

    if (_traceExecution && sql) {
        NSLog(@"%@ executeQuery: %@", self, sql);
    }

    if (_shouldCacheStatements) {
        statement = [self cachedStatementForQuery:sql];
        pStmt = statement ? [statement statement] : 0x00;
        [statement reset];
    }

    if (!pStmt) {
        rc = sqlite3_prepare_v2(_db, [sql UTF8String], -1, &pStmt, 0);

        if (SQLITE_OK != rc) {
            if (_logsErrors) {
                NSLog(@"DB Error: %d \"%@\"", [self lastErrorCode], [self lastErrorMessage]);
                NSLog(@"DB Query: %@", sql);
                NSLog(@"DB Path: %@", _databasePath);
            }

            if (_crashOnErrors) {
                NSAssert(false, @"DB Error: %d \"%@\"", [self lastErrorCode], [self lastErrorMessage]);
                abort();
            }

            sqlite3_finalize(pStmt);
            pStmt = 0x00;
            _isExecutingStatement = NO;
            return nil;
        }
    }

    if (shouldBind) {
        BOOL success = [self bindStatement:pStmt WithArgumentsInArray:arrayArgs orDictionary:dictionaryArgs orVAList:args];
        if (!success) {
            return nil;
        }
    }

    FMDBRetain(statement); // to balance the release below

    if (!statement) {
        statement = [[FMStatement alloc] init];
        [statement setStatement:pStmt];

        if (_shouldCacheStatements && sql) {
            [self setCachedStatement:statement forQuery:sql];
        }
    }

    // the statement gets closed in rs's dealloc or [rs close];
    // we should only autoclose if we're binding automatically when the statement is prepared
    rs = [FMResultSet resultSetWithStatement:statement usingParentDatabase:self shouldAutoClose:shouldBind];
    [rs setQuery:sql];

    NSValue *openResultSet = [NSValue valueWithNonretainedObject:rs];
    [_openResultSets addObject:openResultSet];

    [statement setUseCount:[statement useCount] + 1];

    FMDBRelease(statement);

    _isExecutingStatement = NO;

    return rs;
}

- (BOOL)bindStatement:(sqlite3_stmt *)pStmt WithArgumentsInArray:(NSArray*)arrayArgs orDictionary:(NSDictionary *)dictionaryArgs orVAList:(va_list)args {
    id obj;
    int idx = 0;
    int queryCount = sqlite3_bind_parameter_count(pStmt); // pointed out by Dominic Yu (thanks!)

    // If dictionaryArgs is passed in, that means we are using sqlite's named parameter support
    if (dictionaryArgs) {

        for (NSString *dictionaryKey in [dictionaryArgs allKeys]) {

            // Prefix the key with a colon.
            NSString *parameterName = [[NSString alloc] initWithFormat:@":%@", dictionaryKey];

            if (_traceExecution) {
                NSLog(@"%@ = %@", parameterName, [dictionaryArgs objectForKey:dictionaryKey]);
            }

            // Get the index for the parameter name.
            int namedIdx = sqlite3_bind_parameter_index(pStmt, [parameterName UTF8String]);

            FMDBRelease(parameterName);

            if (namedIdx > 0) {
                // Standard binding from here.
                int rc = [self bindObject:[dictionaryArgs objectForKey:dictionaryKey] toColumn:namedIdx inStatement:pStmt];
                if (rc != SQLITE_OK) {
                    NSLog(@"Error: unable to bind (%d, %s", rc, sqlite3_errmsg(_db));
                    sqlite3_finalize(pStmt);
                    pStmt = 0x00;
                    _isExecutingStatement = NO;
                    return false;
                }
                // increment the binding count, so our check below works out
                idx++;
            }
            else {
                NSLog(@"Could not find index for %@", dictionaryKey);
            }
        }
    }
    else {
        while (idx < queryCount) {
            if (arrayArgs && idx < (int)[arrayArgs count]) {
                obj = [arrayArgs objectAtIndex:(NSUInteger)idx];
            }
            else if (args) {
                obj = va_arg(args, id);
            }
            else {
                //We ran out of arguments
                break;
            }

            if (_traceExecution) {
                if ([obj isKindOfClass:[NSData class]]) {
                    NSLog(@"data: %ld bytes", (unsigned long)[(NSData*)obj length]);
                }
                else {
                    NSLog(@"obj: %@", obj);
                }
            }

            idx++;

            int rc = [self bindObject:obj toColumn:idx inStatement:pStmt];
            if (rc != SQLITE_OK) {
                NSLog(@"Error: unable to bind (%d, %s", rc, sqlite3_errmsg(_db));
                sqlite3_finalize(pStmt);
                pStmt = 0x00;
                _isExecutingStatement = NO;
                return false;
            }
        }
    }

    if (idx != queryCount) {
        NSLog(@"Error: the bind count is not correct for the # of variables (executeQuery)");
        sqlite3_finalize(pStmt);
        pStmt = 0x00;
        _isExecutingStatement = NO;
        return false;
    }

    return true;
}

- (FMResultSet *)executeQuery:(NSString*)sql, ... {
    va_list args;
    va_start(args, sql);

    id result = [self executeQuery:sql withArgumentsInArray:nil orDictionary:nil orVAList:args shouldBind:true];

    va_end(args);
    return result;
}

- (FMResultSet *)executeQueryWithFormat:(NSString*)format, ... {
    va_list args;
    va_start(args, format);

    NSMutableString *sql = [NSMutableString stringWithCapacity:[format length]];
    NSMutableArray *arguments = [NSMutableArray array];
    [self extractSQL:format argumentsList:args intoString:sql arguments:arguments];

    va_end(args);

    return [self executeQuery:sql withArgumentsInArray:arguments];
}

- (FMResultSet *)executeQuery:(NSString *)sql withArgumentsInArray:(NSArray *)arguments {
    return [self executeQuery:sql withArgumentsInArray:arguments orDictionary:nil orVAList:nil shouldBind:true];
}

- (FMResultSet *)executeQuery:(NSString *)sql values:(NSArray *)values error:(NSError * __autoreleasing *)error {
    FMResultSet *rs = [self executeQuery:sql withArgumentsInArray:values orDictionary:nil orVAList:nil shouldBind:true];
    if (!rs && error) {
        *error = [self lastError];
    }
    return rs;
}

- (FMResultSet *)executeQuery:(NSString*)sql withVAList:(va_list)args {
    return [self executeQuery:sql withArgumentsInArray:nil orDictionary:nil orVAList:args shouldBind:true];
}

#pragma mark Execute updates

- (BOOL)executeUpdate:(NSString*)sql error:(NSError * _Nullable __autoreleasing *)outErr withArgumentsInArray:(NSArray*)arrayArgs orDictionary:(NSDictionary *)dictionaryArgs orVAList:(va_list)args {
    FMResultSet *rs = [self executeQuery:sql withArgumentsInArray:arrayArgs orDictionary:dictionaryArgs orVAList:args shouldBind:true];
    if (!rs) {
        if (outErr) {
            *outErr = [self lastError];
        }
        return false;
    }

    return [rs internalStepWithError:outErr] == SQLITE_DONE;
}

- (BOOL)executeUpdate:(NSString*)sql, ... {
    va_list args;
    va_start(args, sql);

    BOOL result = [self executeUpdate:sql error:nil withArgumentsInArray:nil orDictionary:nil orVAList:args];

    va_end(args);
    return result;
}

- (BOOL)executeUpdate:(NSString*)sql withArgumentsInArray:(NSArray *)arguments {
    return [self executeUpdate:sql error:nil withArgumentsInArray:arguments orDictionary:nil orVAList:nil];
}

- (BOOL)executeUpdate:(NSString*)sql values:(NSArray *)values error:(NSError * __autoreleasing *)error {
    return [self executeUpdate:sql error:error withArgumentsInArray:values orDictionary:nil orVAList:nil];
}

- (BOOL)executeUpdate:(NSString*)sql withParameterDictionary:(NSDictionary *)arguments {
    return [self executeUpdate:sql error:nil withArgumentsInArray:nil orDictionary:arguments orVAList:nil];
}

- (BOOL)executeUpdate:(NSString*)sql withVAList:(va_list)args {
    return [self executeUpdate:sql error:nil withArgumentsInArray:nil orDictionary:nil orVAList:args];
}

- (BOOL)executeUpdateWithFormat:(NSString*)format, ... {
    va_list args;
    va_start(args, format);

    NSMutableString *sql      = [NSMutableString stringWithCapacity:[format length]];
    NSMutableArray *arguments = [NSMutableArray array];

    [self extractSQL:format argumentsList:args intoString:sql arguments:arguments];

    va_end(args);

    return [self executeUpdate:sql withArgumentsInArray:arguments];
}


int FMDBExecuteBulkSQLCallback(void *theBlockAsVoid, int columns, char **values, char **names); // shhh clang.
int FMDBExecuteBulkSQLCallback(void *theBlockAsVoid, int columns, char **values, char **names) {

    if (!theBlockAsVoid) {
        return SQLITE_OK;
    }

    int (^execCallbackBlock)(NSDictionary *resultsDictionary) = (__bridge int (^)(NSDictionary *__strong))(theBlockAsVoid);

    NSMutableDictionary *dictionary = [NSMutableDictionary dictionaryWithCapacity:(NSUInteger)columns];

    for (NSInteger i = 0; i < columns; i++) {
        NSString *key = [NSString stringWithUTF8String:names[i]];
        id value = values[i] ? [NSString stringWithUTF8String:values[i]] : [NSNull null];
        value = value ? value : [NSNull null];
        [dictionary setObject:value forKey:key];
    }

    return execCallbackBlock(dictionary);
}

- (BOOL)executeStatements:(NSString *)sql {
    return [self executeStatements:sql withResultBlock:nil];
}

- (BOOL)executeStatements:(NSString *)sql withResultBlock:(__attribute__((noescape)) FMDBExecuteStatementsCallbackBlock)block {

    int rc;
    char *errmsg = nil;

    rc = sqlite3_exec([self sqliteHandle], [sql UTF8String], block ? FMDBExecuteBulkSQLCallback : nil, (__bridge void *)(block), &errmsg);

    if (errmsg && [self logsErrors]) {
        NSLog(@"Error inserting batch: %s", errmsg);
    }
    if (errmsg) {
        sqlite3_free(errmsg);
    }

    return (rc == SQLITE_OK);
}

- (BOOL)executeUpdate:(NSString*)sql withErrorAndBindings:(NSError * _Nullable __autoreleasing *)outErr, ... {

    va_list args;
    va_start(args, outErr);

    BOOL result = [self executeUpdate:sql error:outErr withArgumentsInArray:nil orDictionary:nil orVAList:args];

    va_end(args);
    return result;
}


#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-implementations"
- (BOOL)update:(NSString*)sql withErrorAndBindings:(NSError * _Nullable __autoreleasing *)outErr, ... {
    va_list args;
    va_start(args, outErr);

    BOOL result = [self executeUpdate:sql error:outErr withArgumentsInArray:nil orDictionary:nil orVAList:args];

    va_end(args);
    return result;
}

#pragma clang diagnostic pop

#pragma mark Prepare

- (FMResultSet *)prepare:(NSString *)sql {
    return [self executeQuery:sql withArgumentsInArray:nil orDictionary:nil orVAList:nil shouldBind:false];
}

#pragma mark Transactions

- (BOOL)rollback {
    BOOL b = [self executeUpdate:@"rollback transaction"];

    if (b) {
        _isInTransaction = NO;
    }

    return b;
}

- (BOOL)commit {
    BOOL b =  [self executeUpdate:@"commit transaction"];

    if (b) {
        _isInTransaction = NO;
    }

    return b;
}

- (BOOL)beginTransaction {

    BOOL b = [self executeUpdate:@"begin exclusive transaction"];
    if (b) {
        _isInTransaction = YES;
    }

    return b;
}

- (BOOL)beginDeferredTransaction {

    BOOL b = [self executeUpdate:@"begin deferred transaction"];
    if (b) {
        _isInTransaction = YES;
    }

    return b;
}

- (BOOL)beginImmediateTransaction {

    BOOL b = [self executeUpdate:@"begin immediate transaction"];
    if (b) {
        _isInTransaction = YES;
    }

    return b;
}

- (BOOL)beginExclusiveTransaction {

    BOOL b = [self executeUpdate:@"begin exclusive transaction"];
    if (b) {
        _isInTransaction = YES;
    }

    return b;
}

- (BOOL)inTransaction {
    return _isInTransaction;
}

- (BOOL)interrupt
{
    if (_db) {
        sqlite3_interrupt([self sqliteHandle]);
        return YES;
    }
    return NO;
}

static NSString *FMDBEscapeSavePointName(NSString *savepointName) {
    return [savepointName stringByReplacingOccurrencesOfString:@"'" withString:@"''"];
}

- (BOOL)startSavePointWithName:(NSString*)name error:(NSError * _Nullable __autoreleasing *)outErr {
#if SQLITE_VERSION_NUMBER >= 3007000
    NSParameterAssert(name);

    NSString *sql = [NSString stringWithFormat:@"savepoint '%@';", FMDBEscapeSavePointName(name)];

    return [self executeUpdate:sql error:outErr withArgumentsInArray:nil orDictionary:nil orVAList:nil];
#else
    NSString *errorMessage = NSLocalizedStringFromTable(@"Save point functions require SQLite 3.7", @"FMDB", nil);
    if (self.logsErrors) NSLog(@"%@", errorMessage);
    return NO;
#endif
}

- (BOOL)releaseSavePointWithName:(NSString*)name error:(NSError * _Nullable __autoreleasing *)outErr {
#if SQLITE_VERSION_NUMBER >= 3007000
    NSParameterAssert(name);

    NSString *sql = [NSString stringWithFormat:@"release savepoint '%@';", FMDBEscapeSavePointName(name)];

    return [self executeUpdate:sql error:outErr withArgumentsInArray:nil orDictionary:nil orVAList:nil];
#else
    NSString *errorMessage = NSLocalizedStringFromTable(@"Save point functions require SQLite 3.7", @"FMDB", nil);
    if (self.logsErrors) NSLog(@"%@", errorMessage);
    return NO;
#endif
}

- (BOOL)rollbackToSavePointWithName:(NSString*)name error:(NSError * _Nullable __autoreleasing *)outErr {
#if SQLITE_VERSION_NUMBER >= 3007000
    NSParameterAssert(name);

    NSString *sql = [NSString stringWithFormat:@"rollback transaction to savepoint '%@';", FMDBEscapeSavePointName(name)];

    return [self executeUpdate:sql error:outErr withArgumentsInArray:nil orDictionary:nil orVAList:nil];
#else
    NSString *errorMessage = NSLocalizedStringFromTable(@"Save point functions require SQLite 3.7", @"FMDB", nil);
    if (self.logsErrors) NSLog(@"%@", errorMessage);
    return NO;
#endif
}

- (NSError*)inSavePoint:(__attribute__((noescape)) void (^)(BOOL *rollback))block {
#if SQLITE_VERSION_NUMBER >= 3007000
    static unsigned long savePointIdx = 0;

    NSString *name = [NSString stringWithFormat:@"dbSavePoint%ld", savePointIdx++];

    BOOL shouldRollback = NO;

    NSError *err = 0x00;

    if (![self startSavePointWithName:name error:&err]) {
        return err;
    }

    if (block) {
        block(&shouldRollback);
    }

    if (shouldRollback) {
        // We need to rollback and release this savepoint to remove it
        [self rollbackToSavePointWithName:name error:&err];
    }
    [self releaseSavePointWithName:name error:&err];

    return err;
#else
    NSString *errorMessage = NSLocalizedStringFromTable(@"Save point functions require SQLite 3.7", @"FMDB", nil);
    if (self.logsErrors) NSLog(@"%@", errorMessage);
    return [NSError errorWithDomain:@"FMDatabase" code:0 userInfo:@{NSLocalizedDescriptionKey : errorMessage}];
#endif
}

- (BOOL)checkpoint:(FMDBCheckpointMode)checkpointMode error:(NSError * __autoreleasing *)error {
    return [self checkpoint:checkpointMode name:nil logFrameCount:NULL checkpointCount:NULL error:error];
}

- (BOOL)checkpoint:(FMDBCheckpointMode)checkpointMode name:(NSString *)name error:(NSError * __autoreleasing *)error {
    return [self checkpoint:checkpointMode name:name logFrameCount:NULL checkpointCount:NULL error:error];
}

- (BOOL)checkpoint:(FMDBCheckpointMode)checkpointMode name:(NSString *)name logFrameCount:(int *)logFrameCount checkpointCount:(int *)checkpointCount error:(NSError * __autoreleasing *)error
{
    const char* dbName = [name UTF8String];
#if SQLITE_VERSION_NUMBER >= 3007006
    int err = sqlite3_wal_checkpoint_v2(_db, dbName, checkpointMode, logFrameCount, checkpointCount);
#else
    NSLog(@"sqlite3_wal_checkpoint_v2 unavailable before sqlite 3.7.6. Ignoring checkpoint mode: %d", mode);
    int err = sqlite3_wal_checkpoint(_db, dbName);
#endif
    if(err != SQLITE_OK) {
        if (error) {
            *error = [self lastError];
        }
        if (self.logsErrors) NSLog(@"%@", [self lastErrorMessage]);
        if (self.crashOnErrors) {
            NSAssert(false, @"%@", [self lastErrorMessage]);
            abort();
        }
        return NO;
    } else {
        return YES;
    }
}


#pragma mark Callback function

void FMDBBlockSQLiteCallBackFunction(sqlite3_context *context, int argc, sqlite3_value **argv); // -Wmissing-prototypes
void FMDBBlockSQLiteCallBackFunction(sqlite3_context *context, int argc, sqlite3_value **argv) {
#if ! __has_feature(objc_arc)
    void (^block)(sqlite3_context *context, int argc, sqlite3_value **argv) = (id)sqlite3_user_data(context);
#else
    void (^block)(sqlite3_context *context, int argc, sqlite3_value **argv) = (__bridge id)sqlite3_user_data(context);
#endif
    if (block) {
        @autoreleasepool {
            block(context, argc, argv);
        }
    }
}

// deprecated because "arguments" parameter is not maximum argument count, but actual argument count.

- (void)makeFunctionNamed:(NSString *)name maximumArguments:(int)arguments withBlock:(void (^)(void *context, int argc, void **argv))block {
    [self makeFunctionNamed:name arguments:arguments block:block];
}

- (void)makeFunctionNamed:(NSString *)name arguments:(int)arguments block:(void (^)(void *context, int argc, void **argv))block {

    if (!_openFunctions) {
        _openFunctions = [NSMutableSet new];
    }

    id b = FMDBReturnAutoreleased([block copy]);

    [_openFunctions addObject:b];

    /* I tried adding custom functions to release the block when the connection is destroyed- but they seemed to never be called, so we use _openFunctions to store the values instead. */
#if ! __has_feature(objc_arc)
    sqlite3_create_function([self sqliteHandle], [name UTF8String], arguments, SQLITE_UTF8, (void*)b, &FMDBBlockSQLiteCallBackFunction, 0x00, 0x00);
#else
    sqlite3_create_function([self sqliteHandle], [name UTF8String], arguments, SQLITE_UTF8, (__bridge void*)b, &FMDBBlockSQLiteCallBackFunction, 0x00, 0x00);
#endif
}

- (SqliteValueType)valueType:(void *)value {
    return sqlite3_value_type(value);
}

- (int)valueInt:(void *)value {
    return sqlite3_value_int(value);
}

- (long long)valueLong:(void *)value {
    return sqlite3_value_int64(value);
}

- (double)valueDouble:(void *)value {
    return sqlite3_value_double(value);
}

- (NSData *)valueData:(void *)value {
    const void *bytes = sqlite3_value_blob(value);
    int length = sqlite3_value_bytes(value);
    return bytes ? [NSData dataWithBytes:bytes length:(NSUInteger)length] : nil;
}

- (NSString *)valueString:(void *)value {
    const char *cString = (const char *)sqlite3_value_text(value);
    return cString ? [NSString stringWithUTF8String:cString] : nil;
}

- (void)resultNullInContext:(void *)context {
    sqlite3_result_null(context);
}

- (void)resultInt:(int) value context:(void *)context {
    sqlite3_result_int(context, value);
}

- (void)resultLong:(long long)value context:(void *)context {
    sqlite3_result_int64(context, value);
}

- (void)resultDouble:(double)value context:(void *)context {
    sqlite3_result_double(context, value);
}

- (void)resultData:(NSData *)data context:(void *)context {
    sqlite3_result_blob(context, data.bytes, (int)data.length, SQLITE_TRANSIENT);
}

- (void)resultString:(NSString *)value context:(void *)context {
    sqlite3_result_text(context, [value UTF8String], -1, SQLITE_TRANSIENT);
}

- (void)resultError:(NSString *)error context:(void *)context {
    sqlite3_result_error(context, [error UTF8String], -1);
}

- (void)resultErrorCode:(int)errorCode context:(void *)context {
    sqlite3_result_error_code(context, errorCode);
}

- (void)resultErrorNoMemoryInContext:(void *)context {
    sqlite3_result_error_nomem(context);
}

- (void)resultErrorTooBigInContext:(void *)context {
    sqlite3_result_error_toobig(context);
}

*/
}


// MARK: Busy handler routines

// NOTE: appledoc seems to choke on this function for some reason;
//       so when generating documentation, you might want to ignore the
//       .m files so that it only documents the public interfaces outlined
//       in the .h files.
//
//       This is a known appledoc bug that it has problems with C functions
//       within a class implementation, but for some reason, only this
//       C function causes problems; the rest don't. Anyway, ignoring the .m
//       files with appledoc will prevent this problem from occurring.

func databaseBusyHandler (userdata: UnsafeMutableRawPointer?, count: Int32) -> Int32 {

    guard let databasePointer = userdata else {
        logger.warning("fmdb: databaseBusyHandler missing userdata")
        return 0
    }
    let database = Unmanaged<FMDatabase>.fromOpaque(databasePointer).takeUnretainedValue()


    if count == 0 {
        database._startBusyRetryTime = Date.timeIntervalSinceReferenceDate
        return 1
    }

    let delta = Date.timeIntervalSinceReferenceDate - database._startBusyRetryTime

    if delta < database.maxBusyRetryTimeInterval {
        let requestedSleepInMillseconds = Int32.random(in: 50..<100)
        let actualSleepInMilliseconds = sqlite3_sleep(requestedSleepInMillseconds)
        if actualSleepInMilliseconds != requestedSleepInMillseconds {
            logger.warning("Requested sleep of \(requestedSleepInMillseconds) milliseconds, but SQLite returned \(actualSleepInMilliseconds). Maybe SQLite wasn't built with HAVE_USLEEP=1?")
        }
        return 1
    }

    return 0
}
