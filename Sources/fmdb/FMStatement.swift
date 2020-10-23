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
    internal (set) public var useCount: Int64 = 0

    /** SQL statement */
    internal (set) public var query: String = ""

    /** SQLite sqlite3_stmt

     @see [@c sqlite3_stmt ](https://sqlite.org/c3ref/stmt.html)
     */
    internal (set) public var statement: OpaquePointer? = nil

    /** Indication of whether the statement is in use */
    internal (set) public var inUse = false

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

extension FMStatement: Hashable {

    public func hash (into hasher: inout Hasher) {
        ObjectIdentifier(self).hash(into: &hasher)
    }
}

public func == (lhs: FMStatement, rhs: FMStatement) -> Bool {
    return ObjectIdentifier(lhs) == ObjectIdentifier(rhs)
}
