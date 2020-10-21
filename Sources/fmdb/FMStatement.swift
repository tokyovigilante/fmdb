/** Swift wrapper for @c sqlite3_stmt

 This is a wrapper for a SQLite @c sqlite3_stmt . Generally when using FMDB you will not need to interact directly with `FMStatement`, but rather with @c FMDatabase  and @c FMResultSet  only.

 See also

 - @c FMDatabase
 - @c FMResultSet
 - [@c sqlite3_stmt ](https://sqlite.org/c3ref/stmt.html)
 */
 import CSQLite
 import Foundation

public class FMStatement {

    /** Usage count */
    private (set) public var useCount: Int64 = 0

    /** SQL statement */
    private (set) public var query: String = ""

    /** SQLite sqlite3_stmt

     @see [@c sqlite3_stmt ](https://sqlite.org/c3ref/stmt.html)
     */
    private (set) public var statement: OpaquePointer? = nil

    /** Indication of whether the statement is in use */
    private (set) public var inUse = false

    public func reset () {
        sqlite3_reset(statement)
        inUse = false
    }

    public func close () {
        sqlite3_finalize(statement)
        statement = nil
        inUse = false
    }
    deinit {
        close()
    }

}

extension FMStatement: CustomStringConvertible {
    public var description: String {
        return ("\(useCount) hits for query \(query)")
    }
}
