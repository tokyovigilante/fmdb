//
//  FMDatabaseQueue.h
//  fmdb
//
//  Created by August Mueller on 6/22/11.
//  Copyright 2011 Flying Meat Inc. All rights reserved.
//

/** To perform queries and updates on multiple threads, you'll want to use @c FMDatabaseQueue .

 Using a single instance of @c FMDatabase from multiple threads at once is a bad idea.  It has always been OK to make a @c FMDatabase  object *per thread*.  Just don't share a single instance across threads, and definitely not across multiple threads at the same time.

 Instead, use @c FMDatabaseQueue . Here's how to use it:

 First, make your queue.

@code
FMDatabaseQueue *queue = [FMDatabaseQueue databaseQueueWithPath:aPath];
@endcode

 Then use it like so:

@code
[queue inDatabase:^(FMDatabase *db) {
    [db executeUpdate:@"INSERT INTO myTable VALUES (?)", [NSNumber numberWithInt:1]];
    [db executeUpdate:@"INSERT INTO myTable VALUES (?)", [NSNumber numberWithInt:2]];
    [db executeUpdate:@"INSERT INTO myTable VALUES (?)", [NSNumber numberWithInt:3]];

    FMResultSet *rs = [db executeQuery:@"select * from foo"];
    while ([rs next]) {
        //…
    }
}];
@endcode

 An easy way to wrap things up in a transaction can be done like this:

@code
[queue inTransaction:^(FMDatabase *db, BOOL *rollback) {
    [db executeUpdate:@"INSERT INTO myTable VALUES (?)", [NSNumber numberWithInt:1]];
    [db executeUpdate:@"INSERT INTO myTable VALUES (?)", [NSNumber numberWithInt:2]];
    [db executeUpdate:@"INSERT INTO myTable VALUES (?)", [NSNumber numberWithInt:3]];

    // if (whoopsSomethingWrongHappened) {
    //     *rollback = YES;
    //     return;
    // }

    // etc…
    [db executeUpdate:@"INSERT INTO myTable VALUES (?)", [NSNumber numberWithInt:4]];
}];
@endcode

 @c FMDatabaseQueue will run the blocks on a serialized queue (hence the name of the class).  So if you call @c FMDatabaseQueue 's methods from multiple threads at the same time, they will be executed in the order they are received.  This way queries and updates won't step on each other's toes, and every one is happy.

 @warning Do not instantiate a single @c FMDatabase  object and use it across multiple threads. Use @c FMDatabaseQueue  instead.

 @warning The calls to @c FMDatabaseQueue 's methods are blocking.  So even though you are passing along blocks, they will **not** be run on another thread.

 @sa FMDatabase

 */

import CSQLite
import Foundation

enum FMDBTransaction {
    case exclusive
    case deferred
    case immediate
}

class FMDatabaseQueue {

    private let _queue: DispatchQueue

    private var _db: FMDatabase? = nil

    public var database: FMDatabase? {
        if _db == nil {
            _db = FMDatabase(url: url)
        }
        if !_db!.isOpen {
            let success = _db!.open(flags: openFlags, vfs: vfsName)

            if !success {
                logger.error("FMDatabaseQueue could not reopen database at url \(url?.absoluteString ?? ":memory:")")
                _db = nil
            }
        }
        return _db
    }

    /** Path of database */
    let url: URL?

    /** Open flags */

    let openFlags: Int32

    /**  Custom virtual file system name */
    let vfsName: String?

    ///----------------------------------------------------
    /// @name Initialization, opening, and closing of queue
    ///----------------------------------------------------


    /** Create queue using file URL and specified flags.

     @param url The file path of the database.
     @param openFlags Flags passed to the openWithFlags method of the database.

     @return The @c FMDatabaseQueue  object. @c nil  on error.
     */
    public init? (url: URL? = nil,
            flags: Int32 = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI,
            vfs: String? = nil) {

        let db = FMDatabase(url: url)
        let success = db.open(flags: flags, vfs: vfs)

        if !success {
            logger.error("Could not create database queue for url \(String(describing: url))")
            return nil
        }

        self.url = url
        _queue = DispatchQueue(label: "com.fmdb.database", qos: .default, attributes: [], autoreleaseFrequency: .inherit, target: nil)
        _db = db
        openFlags = flags
        vfsName = vfs
    }

    /** Close database used by queue. */
    public func close () {
        _queue.sync {
            _ = database?.close()
        }
    }

    /** Interupt pending database operation. */
    public func interrupt () throws {
        try database?.interrupt()
    }


    ///-----------------------------------------------
    /// @name Dispatching database operations to queue
    ///-----------------------------------------------

    /** Synchronously perform database operations on queue.

     @param block The code to be run on the queue of @c FMDatabaseQueue
     */
     public func `in` (block: (FMDatabase) -> ()) {
        dispatchPrecondition(condition: .notOnQueue(_queue))

        _queue.sync { [self] in

            guard let db = self.database else {
                return
            }

            block(db)

            if db.hasOpenResultSets {
                logger.warning("There is at least one open result set around after performing [FMDatabaseQueue inDatabase:]")
/*
        #if defined(DEBUG) && DEBUG
                    NSSet *openSetCopy = FMDBReturnAutoreleased([[db valueForKey:@"_openResultSets"] copy]);
                    for (NSValue *rsInWrappedInATastyValueMeal in openSetCopy) {
                        FMResultSet *rs = (FMResultSet *)[rsInWrappedInATastyValueMeal pointerValue];
                        NSLog(@"query: '%@'", [rs query]);
                    }
        #endif*/
            }
        }
    }

    private func begin (transaction: FMDBTransaction, block: (FMDatabase, inout     Bool) -> ()) {
        _queue.sync {
            var shouldRollback = false

            guard let db = self.database else {
                return
            }

            do {
                switch transaction {
                case .exclusive:
                  try db.beginTransaction()
                case .deferred:
                  try db.beginDeferredTransaction()
                case .immediate:
                  try db.beginImmediateTransaction()
                }

                block(db, &shouldRollback)

                if shouldRollback {
                  try db.rollback()
                } else {
                  try db.commit()
                }
            } catch SQLiteError.database(let message) {
                logger.error("\(message)")
            } catch {
                logger.error("begin transaction failed: reason unknown")
            }
        }
    }


    /** Synchronously perform database operations on queue, using transactions.

    @param block The code to be run on the queue of @c FMDatabaseQueue

    @warning    Unlike SQLite's `BEGIN TRANSACTION`, this method currently performs
             an exclusive transaction, not a deferred transaction. This behavior
             is likely to change in future versions of FMDB, whereby this method
             will likely eventually adopt standard SQLite behavior and perform
             deferred transactions. If you really need exclusive tranaction, it is
             recommended that you use `inExclusiveTransaction`, instead, not only
             to make your intent explicit, but also to future-proof your code.

    */
    public func inTransaction (block: (FMDatabase, inout Bool) -> ()) {
        begin(transaction: .exclusive, block: block)
    }

    /** Synchronously perform database operations on queue, using deferred transactions.

    @param block The code to be run on the queue of @c FMDatabaseQueue
    */
    public func inDeferredTransaction (block: (FMDatabase, inout Bool) -> ()) {
        begin(transaction: .deferred, block: block)
    }

    /** Synchronously perform database operations on queue, using exclusive transactions.

    @param block The code to be run on the queue of @c FMDatabaseQueue
    */
    public func inExclusiveTransaction (block: (FMDatabase, inout Bool) -> ()) {
        begin(transaction: .exclusive, block: block)

    }

    /** Synchronously perform database operations on queue, using immediate transactions.

    @param block The code to be run on the queue of @c FMDatabaseQueue
    */
    public func inImmediateTransaction (block: (FMDatabase, inout Bool) -> ()) {
        begin(transaction: .immediate, block: block)

    }

    ///-----------------------------------------------
    /// @name Dispatching database operations to queue
    ///-----------------------------------------------

    /** Synchronously perform database operations using save point.

     @param block The code to be run on the queue of @c FMDatabaseQueue
     */

    private var savePointIdx: UInt64 = 0

    // NOTE: you can not nest these, since calling it will pull another database out of the pool and you'll get a deadlock.
    // If you need to nest, use FMDatabase's startSavePointWithName:error: instead.
    public func inSavePoint (block: (FMDatabase, inout Bool) throws -> ()) rethrows {

        try _queue.sync {
            let name = "savePoint\(savePointIdx += 1)"

            var shouldRollback = false

            guard let db = self.database else {
                throw SQLiteError.database(message: "Missing database")
            }

            try db.start(savePoint: name)
            try block(db, &shouldRollback)
            if shouldRollback {
                // We need to rollback and release this savepoint to remove it
                try db.rollbackTo(savePoint: name)
            }
            try db.release(savePoint: name)
        }
    }

    ///-----------------
    /// @name Checkpoint
    ///-----------------

/** Performs a WAL checkpoint

    @param checkpointMode The checkpoint mode for sqlite3_wal_checkpoint_v2
    @param name The db name for sqlite3_wal_checkpoint_v2
    @param error The NSError corresponding to the error, if any.
    @param logFrameCount If not NULL, then this is set to the total number of frames in the log file or to -1 if the checkpoint could not run because of an error or because the database is not in WAL mode.
    @param checkpointCount If not NULL, then this is set to the total number of checkpointed frames in the log file (including any that were already checkpointed before the function was called) or to -1 if the checkpoint could not run due to an error or because the database is not in WAL mode.
    @return YES on success, otherwise NO.
    */
    public func checkpoint (mode: FMDBCheckpointMode, name: String? = nil,
            logFrameCount: inout Int?, checkpointCount: inout Int?) throws {

        try _queue.sync {
            guard let db = self.database else {
                throw SQLiteError.database(message: "Missing database")
            }
            try db.checkpoint(mode: mode, name: name, logFrameCount: &logFrameCount, checkpointCount: &checkpointCount)
        }
    }

    deinit {
        close()
    }

}
