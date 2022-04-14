import CSQLite
import Foundation
import Logging

let logger = Logger(label: "com.testtoast.fmdb")

let SQLITE_STATIC = unsafeBitCast(0, to: sqlite3_destructor_type.self)
let SQLITE_TRANSIENT = unsafeBitCast(-1, to: sqlite3_destructor_type.self)

/**
Enumeration used in checkpoint methods.
*/
public enum FMDBCheckpointMode: Int {
    case passive  = 0 // SQLITE_CHECKPOINT_PASSIVE,
    case full     = 1 // SQLITE_CHECKPOINT_FULL,
    case restart  = 2 // SQLITE_CHECKPOINT_RESTART,
    case truncate = 3  // SQLITE_CHECKPOINT_TRUNCATE
}

struct Weak<T: AnyObject> {

    weak var value : T?

}

extension Array where Element == Weak<AnyObject> {

    mutating func reap () {
        self = self.filter { nil != $0.value }
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

    private (set) internal var _db: OpaquePointer? = nil

    private var _isExecutingStatement = false

    fileprivate var _startBusyRetryTime: TimeInterval = 0
    private var _openResultSets = [Weak<FMResultSet>]()
    //private var _openFunctions = Set()

    private var sqlitePath: String? {

        guard let url = databaseURL else {
            return ":memory:"
        }
        return url.absoluteString
    }

    ///-----------------
    /// @name Properties
    ///-----------------

    /** Whether should trace execution */
    public var traceExecution = false

    /** Whether checked out or not */
    public var checkedOut = false

    /** Crash on errors */
    public var crashOnErrors = false

    /** Logs errors */
    public var logsErrors = true

    private (set) public var cachedStatements = [String: Set<FMStatement>]()


    /** Identify whether currently in a transaction or not

    @see beginTransaction
    @see beginDeferredTransaction
    @see commit
    @see rollback
    */
    private (set) public var inTransaction: Bool = false

    private (set) public var isOpen: Bool = false

    public init (url: URL?) {
        assert(sqlite3_threadsafe() != 0, "SQLite is not threadsafe, aborting")
        databaseURL = url
     }

    ///-----------------------------------
    /// @name Opening and closing database
    ///-----------------------------------

    /// Is the database open or not?

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
            do {
                try close()
            } catch {
                logger.warning("\(error.localizedDescription)")
            }
        }
        let dbPointer = UnsafeMutablePointer<OpaquePointer?>.allocate(capacity: 1)
        defer {
            dbPointer.deallocate()
        }
        let err = sqlite3_open_v2(databaseURL!.absoluteString, dbPointer, flags, vfs)
        if err != SQLITE_OK {
            logger.error("error opening!: \(err)")
            return false
        }
        _db = dbPointer.pointee

        if maxBusyRetryTimeInterval > 0 {
            // set the handler
            setMaxBusyRetryTimeInterval()
        }

        isOpen = true
        return true
    }


    public func close () throws {

        clearCachedStatements()
        closeOpenResultSets()

        if _db == nil {
            return
        }

        defer {
            _db = nil
            isOpen = false
        }

        var rc: Int32
        var retry: Bool
        var triedFinalizingOpenStatements = false

        repeat {
            retry = false
            rc = sqlite3_close(_db)
            if rc == SQLITE_BUSY || rc == SQLITE_LOCKED {
                if !triedFinalizingOpenStatements {
                    triedFinalizingOpenStatements = true
                    while let pStmt = sqlite3_next_stmt(_db, nil) {
                        logger.warning("Closing leaked statement")
                        sqlite3_finalize(pStmt)
                        retry = true
                    }
                }
            }
            else if rc != SQLITE_OK {
                throw SQLiteError.database(message: "error closing!: \(rc)")
            }
        }
        while retry
    }

    /** Test to see if we have a good connection to the database.

     This will confirm whether:

     - is database open

     - if open, it will try a simple @c SELECT statement and confirm that it succeeds.

     @return @c YES if everything succeeds, @c NO on failure.
     */
    var goodConnection: Bool {

        if !isOpen {
            return false
        }
/*
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
        #else*/
        do {
            let rs = try execute(query: "select name from sqlite_master where type='table'")
            rs.close()
            return true
        } catch {
            logger.error("\(error.localizedDescription)")
        }
        return false
    }


    ///----------------------
    /// @name Perform updates
    ///----------------------

    /** Execute single update statement

    This method executes a single SQL update statement (i.e. any SQL that does not return results, such as @c UPDATE , @c INSERT , or @c DELETE . This method employs [`sqlite3_prepare_v2`](https://sqlite.org/c3ref/prepare.html) and [`sqlite_step`](https://sqlite.org/c3ref/step.html) to perform the update. Unlike the other @c executeUpdate methods, this uses printf-style formatters (e.g. `%s`, `%d`, etc.) to build the SQL.

    The optional values provided to this method should be objects (e.g. @c NSString , @c NSNumber , @c NSNull , @c NSDate , and @c NSData  objects), not fundamental data types (e.g. @c int , @c long , @c NSInteger , etc.). This method automatically handles the aforementioned object types, and all other object types will be interpreted as text values using the object's @c description  method.

    @param sql The SQL to be performed, with optional `?` placeholders.

    @param arguments A @c NSArray  of objects to be used when binding values to the `?` placeholders in the SQL statement.

    @param dictionary A @c NSDictionary of objects keyed by column names that will be used when binding values to the `?` placeholders in the SQL statement.

    @return @c YES upon success; @c NO upon failure. If failed, you can call @c lastError , @c lastErrorCode , or @c lastErrorMessage  for diagnostic information regarding the failure.

    @see lastError
    @see lastErrorCode
    @see lastErrorMessage
    */
    public func execute (update sql: String, arguments: [Any?]? = nil, dictionary paramDict: [String: Any?]? = nil) throws {
        let rs = try execute(query: sql, arguments: arguments, dictionary: paramDict, shouldBind: true)
        let rc = try rs.stepInternal()
        if rc != SQLITE_DONE {
            throw SQLiteError.database(message: "Unexpected return \(rc) executing \(sql)")
        }
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

    @param arguments A @c NSDictionary of objects keyed by column names that will be used when binding values to the `?` placeholders in the SQL statement.

    @return A @c FMResultSet  for the result set upon success; @c nil  upon failure. If failed, you can call @c lastError , @c lastErrorCode , or @c lastErrorMessage  for diagnostic information regarding the failure.

    @see FMResultSet
    @see [`FMResultSet next`](<[FMResultSet next]>)

    */
    public func execute (query sql: String, arguments: [Any?]? = nil, dictionary paramDict: [String: Any?]? = nil) throws -> FMResultSet {
        return try execute(query: sql, arguments: arguments, dictionary: paramDict, shouldBind: true)
    }

    /// Prepare SQL statement.
    ///
    /// @param sql SQL statement to prepare, generally with `?` placeholders.
    public func prepare (sql: String) throws -> FMResultSet {
        throw SQLiteError.database(message: "Unimplemented")
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
    */
    public func beginTransaction () throws {
        try execute(update: "BEGIN TRANSACTION")
        inTransaction = true
    }

    /** Begin a deferred transaction

    @return @c YES on success; @c NO on failure. If failed, you can call @c lastError , @c lastErrorCode , or @c lastErrorMessage  for diagnostic information regarding the failure.

    @see commit
    @see rollback
    @see beginTransaction
    @see isInTransaction
    */
    public func beginDeferredTransaction () throws {
        try execute(update: "BEGIN DEFERRED TRANSACTION")
        inTransaction = true
    }

    /** Begin an immediate transaction

    @return @c YES on success; @c NO on failure. If failed, you can call @c lastError , @c lastErrorCode , or @c lastErrorMessage  for diagnostic information regarding the failure.

    @see commit
    @see rollback
    @see beginTransaction
    @see isInTransaction
    */

    public func beginImmediateTransaction () throws {
        try execute(update: "BEGIN IMMEDIATE TRANSACTION")
        inTransaction = true
    }

    /** Begin an exclusive transaction

    @return @c YES on success; @c NO on failure. If failed, you can call @c lastError , @c lastErrorCode , or @c lastErrorMessage  for diagnostic information regarding the failure.

    @see commit
    @see rollback
    @see beginTransaction
    @see isInTransaction
    */
    public func beginExclusiveTransaction () throws {
        try execute(update: "BEGIN EXCLUSIVE TRANSACTION")
        inTransaction = true
    }

    /** Commit a transaction

    Commit a transaction that was initiated with either `<beginTransaction>` or with `<beginDeferredTransaction>`.

    @return @c YES on success; @c NO on failure. If failed, you can call @c lastError , @c lastErrorCode , or @c lastErrorMessage  for diagnostic information regarding the failure.

    @see beginTransaction
    @see beginDeferredTransaction
    @see rollback
    @see isInTransaction
    */
    public func commit () throws {
        try execute(update: "COMMIT TRANSACTION")
    }

    /** Rollback a transaction

    Rollback a transaction that was initiated with either `<beginTransaction>` or with `<beginDeferredTransaction>`.

    @return @c YES on success; @c NO on failure. If failed, you can call @c lastError , @c lastErrorCode , or @c lastErrorMessage  for diagnostic information regarding the failure.

    @see beginTransaction
    @see beginDeferredTransaction
    @see commit
    @see isInTransaction
    */
    public func rollback () throws {
        try execute(update: "ROLLBACK TRANSACTION")
        inTransaction = false
    }

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

    private func clearCachedStatements () {
    // FIXME: needs work
            for statements in cachedStatements.enumerated() {
                for statement in statements.element.value {
                    statement.close()
                }
            }
            cachedStatements.removeAll()
        }

    private func cachedStatement (for query: String) -> FMStatement? {

        guard let statements = cachedStatements[query] else {
            return nil
        }

        return statements.filter { !$0.inUse }.randomElement()
    }

    private func cache (statement: FMStatement, for query: String) {

        statement.query = query

        var statements = cachedStatements[query] ?? Set()

        statements.insert(statement)

        cachedStatements[query] = statements
    }

    /** Close all open result sets */
    private func closeOpenResultSets () {
// FIXME: needs work
        while !_openResultSets.isEmpty {
            if let rs = _openResultSets.popLast()?.value {
                rs.parentDB = nil
                rs.close()
            }
        }
    }
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
    public func interrupt () throws {
        if _db == nil {
            throw SQLiteError.database(message: "interrupt called without open DB")
        }
        sqlite3_interrupt(_db)
    }

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
    public var lastErrorMessage: String? {
        return String(cString: sqlite3_errmsg(_db), encoding: .utf8)
    }

    /** Last error code

    Returns the numeric result code or extended result code for the most recent failed SQLite API call associated with a database connection. If a prior API call failed but the most recent API call succeeded, this return value is undefined.

    @return Integer value of the last error code.

    @see [sqlite3_errcode()](https://sqlite.org/c3ref/errcode.html)
    @see lastErrorMessage
    @see lastError

    */
    public var lastErrorCode: Int32 {
        return sqlite3_errcode(_db)
    }


    /** Last extended error code

    Returns the numeric extended result code for the most recent failed SQLite API call associated with a database connection. If a prior API call failed but the most recent API call succeeded, this return value is undefined.

    @return Integer value of the last extended error code.

    @see [sqlite3_errcode()](https://sqlite.org/c3ref/errcode.html)
    @see [2. Primary Result Codes versus Extended Result Codes](https://sqlite.org/rescode.html#primary_result_codes_versus_extended_result_codes)
    @see [5. Extended Result Code List](https://sqlite.org/rescode.html#extrc)
    @see lastErrorMessage
    @see lastError

    */
    public var varlastExtendedErrorCode: Int32 {
        return sqlite3_errcode(_db)
    }

    /** Had error

    @return @c YES if there was an error, @c NO if no error.

    @see lastError
    @see lastErrorCode
    @see lastErrorMessage

    */
    public var hadError: Bool {
        let lastErrCode = lastErrorCode
        return lastErrCode > SQLITE_OK && lastErrCode < SQLITE_ROW
    }

    /** Last error

    @return @c NSError  representing the last error.

    @see lastErrorCode
    @see lastErrorMessage

    */



    public var lastError: Error? {
        if let message = lastErrorMessage {
            return SQLiteError.database(message: "FMDatabase: \(lastErrorCode): \(message)")
        }
        return nil
    }

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
*/
    ///---------------------
    /// @name Date formatter
    ///---------------------


    /** Set a date formatter to use string dates with sqlite instead of the default UNIX timestamps.

    @param format Set to nil to use UNIX timestamps. Defaults to nil. Should be set using a formatter generated using @c FMDatabase:storeableDateFormat .

    @see hasDateFormatter
    @see setDateFormat:
    @see dateFromString:
    @see stringFromDate:
    @see storeableDateFormat:

    @warning Note there is no direct getter for the @c NSDateFormatter , and you should not use the formatter you pass to FMDB for other purposes, as @c NSDateFormatter  is not thread-safe.
    */
    public var dateFormatter: DateFormatter? = nil

/*

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

*/
    func warnInUse () {
        logger.warning("Database is currently in use.")

        if crashOnErrors {
            assert(false, "Database is currently in use.")
            abort()
        }
    }

    var databaseExists: Bool {

        if !isOpen {
            logger.info("The FMDatabase %@ is not open.")
            if crashOnErrors {
                assert(false, "The FMDatabase %@ is not open.")
                abort()
            }
            return false
        }
        return true
    }
/*


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
*/
// MARK: SQL manipulation

    func bind (object: Any?, to idx: Int32, in pStmt: OpaquePointer?) -> Int32 {

        if object == nil {
            return sqlite3_bind_null(pStmt, idx)
        }

        switch object {
        case let data as Data:
            return data.withUnsafeBytes {
                sqlite3_bind_blob(pStmt, idx, $0.baseAddress, Int32(data.count), SQLITE_TRANSIENT)
            }
        case let date as Date:
            if let formatter = dateFormatter {
                return sqlite3_bind_text(pStmt, idx, formatter.string(from: date), -1, SQLITE_TRANSIENT)
            } else {
                return sqlite3_bind_double(pStmt, idx, date.timeIntervalSince1970)
            }
        case let number as Int:
            return sqlite3_bind_int64(pStmt, idx, Int64(number))
        case let number as UInt:
            return sqlite3_bind_int64(pStmt, idx, Int64(number))
        case let number as Int8:
            return sqlite3_bind_int(pStmt, idx, Int32(number))
        case let number as UInt8:
            return sqlite3_bind_int(pStmt, idx, Int32(number))
        case let number as Int16:
            return sqlite3_bind_int(pStmt, idx, Int32(number))
        case let number as UInt16:
            return sqlite3_bind_int(pStmt, idx, Int32(number))
        case let number as Int32:
            return sqlite3_bind_int(pStmt, idx, number)
        case let number as UInt32:
            return sqlite3_bind_int(pStmt, idx, Int32(number))
        case let number as Int64:
            return sqlite3_bind_int64(pStmt, idx, number)
        case let number as UInt64:
            return sqlite3_bind_int64(pStmt, idx, Int64(number))
        case let number as Float:
            return sqlite3_bind_double(pStmt, idx, Double(number))
        case let number as Double:
            return sqlite3_bind_double(pStmt, idx, number)
        case let number as Bool:
            return sqlite3_bind_int(pStmt, idx, number ? 1 : 0)
        case let string as String:
            return sqlite3_bind_text(pStmt, idx, string, -1, SQLITE_TRANSIENT)
        case let url as URL:
            return sqlite3_bind_text(pStmt, idx, url.absoluteString, -1, SQLITE_TRANSIENT)
        case let uuid as UUID:
            return sqlite3_bind_text(pStmt, idx, uuid.uuidString, -1, SQLITE_TRANSIENT)
        default:
            logger.warning("Unknown object \(String(describing: object))")
        }
        return sqlite3_bind_text(pStmt, idx, String(describing: object), -1, SQLITE_TRANSIENT)
    }

/*
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
*/
    // MARK: Execute queries

    private func execute (query sql: String, arguments: [Any?]?,
            dictionary: [String: Any?]?, shouldBind: Bool) throws -> FMResultSet {

        if !databaseExists {
            throw SQLiteError.database(message: "Database does not exist")
        }

        if _isExecutingStatement {
            warnInUse()
            throw SQLiteError.database(message: "Database executing statement")
        }

        _isExecutingStatement = true

        var rc: Int32 = SQLITE_OK

        var pStmt: OpaquePointer? = nil

        if traceExecution {
            logger.trace("executeQuery: \(sql)")
        }

        var statement: FMStatement! = nil

        if shouldCacheStatements {
            if let cachedStatement = cachedStatement(for: sql) {
                pStmt = cachedStatement.statement
                cachedStatement.reset()
                statement = cachedStatement
            }
        }
        if pStmt == nil {
            rc = sqlite3_prepare_v2(_db, sql, -1, &pStmt, nil)

            if rc != SQLITE_OK {
                let message = "DB query error: \(lastErrorCode): \(lastErrorMessage ?? "Unknown error"), query \(sql)"
                if logsErrors {
                    logger.error("\(message)")
                }

                if crashOnErrors {
                    abort()
                }
                sqlite3_finalize(pStmt)
                _isExecutingStatement = false
                throw SQLiteError.database(message: "DB query error: \(lastErrorCode): \(lastErrorMessage ?? "Unknown error")")
            }
        }

        if shouldBind {
            try bind(statement: pStmt, arguments: arguments, dictionary: dictionary)
        }

        if statement == nil {
            statement = FMStatement()
            statement.statement = pStmt

            if shouldCacheStatements {
                cache(statement: statement, for: sql)
            }
        }

        // the statement gets closed in rs's dealloc or [rs close];
        // we should only autoclose if we're binding automatically when the statement is prepared
        let rs = FMResultSet(statement: statement, parentDatabase: self, shouldAutoClose: shouldBind)
        rs.query = sql

        _openResultSets.append(Weak<FMResultSet>(value: rs))
        statement.useCount += 1

        _isExecutingStatement = false

        return rs

    }

    private func bind (statement pStmt: OpaquePointer?, arguments: [Any?]? = nil, dictionary dictionaryArgs: [String: Any?]? = nil) throws {

        //id obj;
        var idx = 0

        let queryCount = sqlite3_bind_parameter_count(pStmt) // pointed out by Dominic Yu (thanks!)

        // If dictionaryArgs is passed in, that means we are using sqlite's named parameter Support
        if let dictionaryArgs = dictionaryArgs {

            for (key, arg) in dictionaryArgs {

                // Prefix the key with a colon.
                let parameterName = ":" + key

                if traceExecution {
                    logger.trace("\(parameterName) = \(key)")
                }

                // Get the index for the parameter name.
                let namedIdx = sqlite3_bind_parameter_index(pStmt, parameterName)

                if namedIdx > 0 {
                    // Standard binding from here.
                    let rc = bind(object: arg, to: namedIdx, in: pStmt)
                    if rc != SQLITE_OK {
                        sqlite3_finalize(pStmt)
                        _isExecutingStatement = false
                        throw SQLiteError.database(message: "Unable to bind (\(rc): \(lastErrorMessage ?? "unknown error")")
                    }
                    // increment the binding count, so our check below works out
                    idx += 1
                }
                else {
                    logger.warning("Could not find index for \(key)")
                }
            }
        }
        else if let arguments = arguments {
            var obj: Any?
            while idx < queryCount {
                if idx < arguments.count {
                    obj = arguments[idx]
                } else {
                    //We ran out of arguments
                    break
                }

                if traceExecution {
                    if let object = obj as?Data {
                        logger.trace("data: \(object.count) bytes")
                    } else {
                        logger.trace("obj: \(String(describing: obj))")
                    }
                }

                idx += 1

                let rc = bind(object: obj, to: Int32(idx), in: pStmt)
                if rc != SQLITE_OK {
                    sqlite3_finalize(pStmt)
                    _isExecutingStatement = false
                    throw SQLiteError.database(message: "Unable to bind (\(rc), \(lastErrorMessage ?? "unknown error")")
                }
            }
        }

        if idx != queryCount {
            sqlite3_finalize(pStmt)
            _isExecutingStatement = false
            throw SQLiteError.database(message: "Error: the bind count is not correct for the # of variables (executeQuery)")
        }
    }
/*

- (FMResultSet *)executeQueryWithFormat:(NSString*)format, ... {
    va_list args;
    va_start(args, format);

    NSMutableString *sql = [NSMutableString stringWithCapacity:[format length]];
    NSMutableArray *arguments = [NSMutableArray array];
    [self extractSQL:format argumentsList:args intoString:sql arguments:arguments];

    va_end(args);

    return [self executeQuery:sql withArgumentsInArray:arguments];
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
