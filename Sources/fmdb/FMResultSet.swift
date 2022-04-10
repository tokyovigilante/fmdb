import CSQLite
import Foundation
import Logging

/** Represents the results of executing a query on an @c FMDatabase .

 See also

 - @c FMDatabase
 */
public class FMResultSet {


    internal (set) public var parentDB: FMDatabase?

    ///-----------------
    /// @name Properties
    ///-----------------

    /** Executed query */
    internal (set) public var query: String = ""

    /** `FMStatement` used by result set. */
    let statement: FMStatement

    let shouldAutoClose: Bool

    /** `NSMutableDictionary` mapping column names to numeric index */
    lazy var columnNameToIndexMap: [String: Int] = { () -> [String: Int] in
        let count = sqlite3_column_count(statement.statement)
        var map: [String: Int] = [:]

        for i in 0..<count {
            guard let name = String(cString: sqlite3_column_name(statement.statement, i), encoding: .utf8) else {
                continue
            }
            map[name] = Int(i)
        }
        return map
    }()

    ///------------------------------------
    /// @name Creating and closing a result set
    ///------------------------------------

    internal init (statement: FMStatement, parentDatabase: FMDatabase, shouldAutoClose autoClose: Bool) {

        self.statement = statement
        self.parentDB = parentDatabase
        self.shouldAutoClose = autoClose

        assert(!statement.inUse)
        statement.inUse = true
    }

    /** Close result set */

    public func close () {
        statement.reset()
        parentDB = nil
    }

    ///---------------------------------------
    /// @name Iterating through the result set
    ///---------------------------------------

    /** Retrieve next row for result set.

     You must always invoke `next` or `nextWithError` before attempting to access the values returned in a query, even if you're only expecting one.

     @return if row successfully retrieved; throws SQLiteError.sqliteDone if end of result set reached

     @see hasAnotherRow
     */
    public func next () throws {
        do {
            try stepInternal()
        } catch SQLiteError.sqliteRow {
            return
        } catch {
            throw error
        }
    }

    /** Perform SQL statement.

     @param outErr A 'NSError' object to receive any error object (if any).

     @return if success; throws SQLiteError.sqliteRow if another row present

     @see hasAnotherRow
    */
    public func step () throws {
        do {
            try stepInternal()
        } catch SQLiteError.sqliteRow {
            return
        } catch {
            throw error
        }
    }

    internal func stepInternal () throws {

        let rc = sqlite3_step(statement.statement)

        defer {
            if rc != SQLITE_ROW && shouldAutoClose {
                close()
            }
        }

        switch rc {
            case SQLITE_BUSY, SQLITE_LOCKED:
                throw SQLiteError.database(message: "SQLite database busy or locked during FMRResultSet step")
            case SQLITE_ROW:
                throw SQLiteError.sqliteRow // Not an error but can be detected if required by calling function
            case SQLITE_DONE:
                throw SQLiteError.sqliteDone // Not an error but can be detected if required by calling function
            case SQLITE_ERROR, SQLITE_MISUSE:
                if let parentDB = parentDB {
                    let message = String(cString: sqlite3_errmsg(parentDB._db), encoding: .utf8)
                    throw SQLiteError.database(message: "FMResultSet: sqlite3_step() failed: \(rc) - \(String(describing: message))")
                } else {
                    throw SQLiteError.database(message: "FMResultSet: sqlite3_step() failed, error \(rc), parentDB does not exist")
                }
            default:
                throw SQLiteError.database(message: "FMResultSet: sqlite3_step() failed")
        }
    }
/*
    /** Did the last call to `<next>` succeed in retrieving another row?

     @return 'YES' if there is another row; 'NO' if not.

     @see next

     @warning The `hasAnotherRow` method must follow a call to `<next>`. If the previous database interaction was something other than a call to `next`, then this method may return @c NO, whether there is another row of data or not.
     */

    - (BOOL)hasAnotherRow;

    ///---------------------------------------------
    /// @name Retrieving information from result set
    ///---------------------------------------------

    /** How many columns in result set

     @return Integer value of the number of columns.
     */

    @property (nonatomic, readonly) int columnCount;

    /** Column index for column name

     @param columnName @c NSString  value of the name of the column.

     @return Zero-based index for column.
     */
*/
    public func columnIndex (for name: String) -> Int {

        let name = name.lowercased()

        if let index = columnNameToIndexMap[name] {
            return index
        }
        logger.warning("No column named \(name) found")
        return -1
    }

    /** Column name for column index

     @param columnIdx Zero-based index for column.

     @return columnName @c NSString  value of the name of the column.
     */
/*
    - (NSString * _Nullable)columnNameForIndex:(int)columnIdx;

    /** Result set integer value for column.

     @param columnName @c NSString  value of the name of the column.

     @return @c int  value of the result set's column.
     */

    - (int)intForColumn:(NSString*)columnName;
*/
    /** Result set integer value for column.

     @param columnIdx Zero-based index for column.

     @return @c int  value of the result set's column.
     */

    public func int (columnIndex i: Int) -> Int32 {
        return sqlite3_column_int(statement.statement, Int32(i))
    }
/*
    /** Result set @c long  value for column.

     @param columnName @c NSString  value of the name of the column.

     @return @c long  value of the result set's column.
     */

    - (long)longForColumn:(NSString*)columnName;

    /** Result set long value for column.

     @param columnIdx Zero-based index for column.

     @return @c long  value of the result set's column.
     */

    - (long)longForColumnIndex:(int)columnIdx;

    /** Result set `long long int` value for column.

     @param columnName @c NSString  value of the name of the column.

     @return `long long int` value of the result set's column.
     */

    - (long long int)longLongIntForColumn:(NSString*)columnName;

    /** Result set `long long int` value for column.

     @param columnIdx Zero-based index for column.

     @return `long long int` value of the result set's column.
     */

    - (long long int)longLongIntForColumnIndex:(int)columnIdx;

    /** Result set `unsigned long long int` value for column.

     @param columnName @c NSString  value of the name of the column.

     @return `unsigned long long int` value of the result set's column.
     */

    - (unsigned long long int)unsignedLongLongIntForColumn:(NSString*)columnName;

    /** Result set `unsigned long long int` value for column.

     @param columnIdx Zero-based index for column.

     @return `unsigned long long int` value of the result set's column.
     */

    - (unsigned long long int)unsignedLongLongIntForColumnIndex:(int)columnIdx;

    /** Result set `BOOL` value for column.

     @param columnName @c NSString  value of the name of the column.

     @return `BOOL` value of the result set's column.
     */

    - (BOOL)boolForColumn:(NSString*)columnName;

    /** Result set `BOOL` value for column.

     @param columnIdx Zero-based index for column.

     @return `BOOL` value of the result set's column.
     */

    - (BOOL)boolForColumnIndex:(int)columnIdx;

    /** Result set `double` value for column.

     @param columnName @c NSString  value of the name of the column.

     @return `double` value of the result set's column.

     */

    - (double)doubleForColumn:(NSString*)columnName;

    /** Result set `double` value for column.

     @param columnIdx Zero-based index for column.

     @return `double` value of the result set's column.

     */

    - (double)doubleForColumnIndex:(int)columnIdx;

    /** Result set @c NSString  value for column.

     @param columnName @c NSString  value of the name of the column.

     @return String value of the result set's column.

     */
    */
    public func string (column name: String) -> String? {
        return string(column: columnIndex(for: name))

    }

    public func string (column index: Int) -> String? {

        if sqlite3_column_type(statement.statement, Int32(index)) == SQLITE_NULL ||
                index < 0 ||
                index >= sqlite3_column_count(statement.statement) {
            return nil
        }
        guard let c = sqlite3_column_text(statement.statement, Int32(index)) else {
            return nil
        }
        return String(cString: c)
    }

    /** Result set @c NSString  value for column.

     @param columnIdx Zero-based index for column.

     @return String value of the result set's column.
     */
    /*
    - (NSString * _Nullable)stringForColumnIndex:(int)columnIdx;

    /** Result set @c NSDate  value for column.

     @param columnName @c NSString  value of the name of the column.

     @return Date value of the result set's column.
     */

    - (NSDate * _Nullable)dateForColumn:(NSString*)columnName;

    /** Result set @c NSDate  value for column.

     @param columnIdx Zero-based index for column.

     @return Date value of the result set's column.

     */

    - (NSDate * _Nullable)dateForColumnIndex:(int)columnIdx;

    /** Result set @c NSData  value for column.

     This is useful when storing binary data in table (such as image or the like).

     @param columnName @c NSString  value of the name of the column.

     @return Data value of the result set's column.

     */

    - (NSData * _Nullable)dataForColumn:(NSString*)columnName;

    /** Result set @c NSData  value for column.

     @param columnIdx Zero-based index for column.

     @return Data value of the result set's column.
     */

    - (NSData * _Nullable)dataForColumnIndex:(int)columnIdx;

    /** Result set `(const unsigned char *)` value for column.

     @param columnName @c NSString  value of the name of the column.

     @return `(const unsigned char *)` value of the result set's column.
     */


    /** Result set `(const unsigned char *)` value for column.

     @param columnIdx Zero-based index for column.

     @return `(const unsigned char *)` value of the result set's column.
     */

    - (const unsigned char * _Nullable)UTF8StringForColumnIndex:(int)columnIdx;

    /** Result set object for column.

     @param columnName Name of the column.

     @return Either @c NSNumber , @c NSString , @c NSData , or @c NSNull . If the column was @c NULL , this returns `[NSNull null]` object.

     @see objectForKeyedSubscript:
     */

    - (id _Nullable)objectForColumn:(NSString*)columnName;

    - (id _Nullable)objectForColumnName:(NSString*)columnName __deprecated_msg("Use objectForColumn instead");

    /** Result set object for column.

     @param columnIdx Zero-based index for column.

     @return Either @c NSNumber , @c NSString , @c NSData , or @c NSNull . If the column was @c NULL , this returns `[NSNull null]` object.

     @see objectAtIndexedSubscript:
     */

    - (id _Nullable)objectForColumnIndex:(int)columnIdx;

    /** Result set object for column.

     This method allows the use of the "boxed" syntax supported in Modern Objective-C. For example, by defining this method, the following syntax is now supported:

    @code
    id result = rs[@"employee_name"];
    @endcode

     This simplified syntax is equivalent to calling:

    @code
    id result = [rs objectForKeyedSubscript:@"employee_name"];
    @endcode

     which is, it turns out, equivalent to calling:

    @code
    id result = [rs objectForColumnName:@"employee_name"];
    @endcode

     @param columnName @c NSString  value of the name of the column.

     @return Either @c NSNumber , @c NSString , @c NSData , or @c NSNull . If the column was @c NULL , this returns `[NSNull null]` object.
     */

    - (id _Nullable)objectForKeyedSubscript:(NSString *)columnName;

    /** Result set object for column.

     This method allows the use of the "boxed" syntax supported in Modern Objective-C. For example, by defining this method, the following syntax is now supported:

    @code
    id result = rs[0];
    @endcode

     This simplified syntax is equivalent to calling:

    @code
    id result = [rs objectForKeyedSubscript:0];
    @endcode

     which is, it turns out, equivalent to calling:

    @code
    id result = [rs objectForColumnName:0];
    @endcode

     @param columnIdx Zero-based index for column.

     @return Either @c NSNumber , @c NSString , @c NSData , or @c NSNull . If the column was @c NULL , this returns `[NSNull null]` object.
     */

    - (id _Nullable)objectAtIndexedSubscript:(int)columnIdx;

    /** Result set @c NSData  value for column.

     @param columnName @c NSString  value of the name of the column.

     @return Data value of the result set's column.

     @warning If you are going to use this data after you iterate over the next row, or after you close the
    result set, make sure to make a copy of the data first (or just use `<dataForColumn:>`/`<dataForColumnIndex:>`)
    If you don't, you're going to be in a world of hurt when you try and use the data.

     */

    - (NSData * _Nullable)dataNoCopyForColumn:(NSString *)columnName NS_RETURNS_NOT_RETAINED;

    /** Result set @c NSData  value for column.

     @param columnIdx Zero-based index for column.

     @return Data value of the result set's column.

     @warning If you are going to use this data after you iterate over the next row, or after you close the
     result set, make sure to make a copy of the data first (or just use `<dataForColumn:>`/`<dataForColumnIndex:>`)
     If you don't, you're going to be in a world of hurt when you try and use the data.

     */

    - (NSData * _Nullable)dataNoCopyForColumnIndex:(int)columnIdx NS_RETURNS_NOT_RETAINED;

    /** Is the column @c NULL ?

     @param columnIdx Zero-based index for column.

     @return @c YES if column is @c NULL ; @c NO if not @c NULL .
     */

    - (BOOL)columnIndexIsNull:(int)columnIdx;

    /** Is the column @c NULL ?

     @param columnName @c NSString  value of the name of the column.

     @return @c YES if column is @c NULL ; @c NO if not @c NULL .
     */

    - (BOOL)columnIsNull:(NSString*)columnName;


    /** Returns a dictionary of the row results mapped to case sensitive keys of the column names.

     @warning The keys to the dictionary are case sensitive of the column names.
     */

    @property (nonatomic, readonly, nullable) NSDictionary *resultDictionary;

    /** Returns a dictionary of the row results

     @see resultDictionary

     @warning **Deprecated**: Please use `<resultDictionary>` instead.  Also, beware that `<resultDictionary>` is case sensitive!
     */

    - (NSDictionary * _Nullable)resultDict __deprecated_msg("Use resultDictionary instead");

    ///-----------------------------
    /// @name Key value coding magic
    ///-----------------------------

    /** Performs `setValue` to yield support for key value observing.

     @param object The object for which the values will be set. This is the key-value-coding compliant object that you might, for example, observe.

     */

    - (void)kvcMagic:(id)object;

    ///-----------------------------
    /// @name Binding values
    ///-----------------------------

    /// Bind array of values to prepared statement.
    ///
    /// @param array Array of values to bind to SQL statement.

    - (BOOL)bindWithArray:(NSArray*)array;

    /// Bind dictionary of values to prepared statement.
    ///
    /// @param dictionary Dictionary of values to bind to SQL statement.

    - (BOOL)bindWithDictionary:(NSDictionary *)dictionary;

    @end

    NS_ASSUME_NONNULL_END

    @end

    #import "FMResultSet.h"
    #import "FMDatabase.h"
    #import <unistd.h>

    #if FMDB_SQLITE_STANDALONE
    #import <sqlite3/sqlite3.h>
    #else
    #import <sqlite3.h>
    #endif

    // MARK: - FMDatabase Private Extension

    @interface FMDatabase ()
    - (void)resultSetDidClose:(FMResultSet *)resultSet;
    - (BOOL)bindStatement:(sqlite3_stmt *)pStmt WithArgumentsInArray:(NSArray*)arrayArgs orDictionary:(NSDictionary *)dictionaryArgs orVAList:(va_list)args;
    @end



    // MARK: - FMResultSet



    - (int)columnCount {
        return sqlite3_column_count([_statement statement]);
    }


    - (void)kvcMagic:(id)object {

        int columnCount = sqlite3_column_count([_statement statement]);

        int columnIdx = 0;
        for (columnIdx = 0; columnIdx < columnCount; columnIdx++) {

            const char *c = (const char *)sqlite3_column_text([_statement statement], columnIdx);

            // check for a null row
            if (c) {
                NSString *s = [NSString stringWithUTF8String:c];

                [object setValue:s forKey:[NSString stringWithUTF8String:sqlite3_column_name([_statement statement], columnIdx)]];
            }
        }
    }

    #pragma clang diagnostic push
    #pragma clang diagnostic ignored "-Wdeprecated-implementations"

    - (NSDictionary *)resultDict {

        NSUInteger num_cols = (NSUInteger)sqlite3_data_count([_statement statement]);

        if (num_cols > 0) {
            NSMutableDictionary *dict = [NSMutableDictionary dictionaryWithCapacity:num_cols];

            NSEnumerator *columnNames = [[self columnNameToIndexMap] keyEnumerator];
            NSString *columnName = nil;
            while ((columnName = [columnNames nextObject])) {
                id objectValue = [self objectForColumnName:columnName];
                [dict setObject:objectValue forKey:columnName];
            }

            return FMDBReturnAutoreleased([dict copy]);
        }
        else {
            NSLog(@"Warning: There seem to be no columns in this set.");
        }

        return nil;
    }

    #pragma clang diagnostic pop

    - (NSDictionary*)resultDictionary {

        NSUInteger num_cols = (NSUInteger)sqlite3_data_count([_statement statement]);

        if (num_cols > 0) {
            NSMutableDictionary *dict = [NSMutableDictionary dictionaryWithCapacity:num_cols];

            int columnCount = sqlite3_column_count([_statement statement]);

            int columnIdx = 0;
            for (columnIdx = 0; columnIdx < columnCount; columnIdx++) {

                NSString *columnName = [NSString stringWithUTF8String:sqlite3_column_name([_statement statement], columnIdx)];
                id objectValue = [self objectForColumnIndex:columnIdx];
                [dict setObject:objectValue forKey:columnName];
            }

            return dict;
        }
        else {
            NSLog(@"Warning: There seem to be no columns in this set.");
        }

        return nil;
    }



    - (BOOL)hasAnotherRow {
        return sqlite3_errcode([_parentDB sqliteHandle]) == SQLITE_ROW;
    }


    - (int)intForColumn:(NSString*)columnName {
        return [self intForColumnIndex:[self columnIndexForName:columnName]];
    }



    - (long)longForColumn:(NSString*)columnName {
        return [self longForColumnIndex:[self columnIndexForName:columnName]];
    }

    - (long)longForColumnIndex:(int)columnIdx {
        return (long)sqlite3_column_int64([_statement statement], columnIdx);
    }

    - (long long int)longLongIntForColumn:(NSString*)columnName {
        return [self longLongIntForColumnIndex:[self columnIndexForName:columnName]];
    }

    - (long long int)longLongIntForColumnIndex:(int)columnIdx {
        return sqlite3_column_int64([_statement statement], columnIdx);
    }

    - (unsigned long long int)unsignedLongLongIntForColumn:(NSString*)columnName {
        return [self unsignedLongLongIntForColumnIndex:[self columnIndexForName:columnName]];
    }

    - (unsigned long long int)unsignedLongLongIntForColumnIndex:(int)columnIdx {
        return (unsigned long long int)[self longLongIntForColumnIndex:columnIdx];
    }

    - (BOOL)boolForColumn:(NSString*)columnName {
        return [self boolForColumnIndex:[self columnIndexForName:columnName]];
    }

    - (BOOL)boolForColumnIndex:(int)columnIdx {
        return ([self intForColumnIndex:columnIdx] != 0);
    }

    - (double)doubleForColumn:(NSString*)columnName {
        return [self doubleForColumnIndex:[self columnIndexForName:columnName]];
    }

    - (double)doubleForColumnIndex:(int)columnIdx {
        return sqlite3_column_double([_statement statement], columnIdx);
    }


    - (NSDate*)dateForColumn:(NSString*)columnName {
        return [self dateForColumnIndex:[self columnIndexForName:columnName]];
    }

    - (NSDate*)dateForColumnIndex:(int)columnIdx {

        if (sqlite3_column_type([_statement statement], columnIdx) == SQLITE_NULL || (columnIdx < 0) || columnIdx >= sqlite3_column_count([_statement statement])) {
            return nil;
        }

        return [_parentDB hasDateFormatter] ? [_parentDB dateFromString:[self stringForColumnIndex:columnIdx]] : [NSDate dateWithTimeIntervalSince1970:[self doubleForColumnIndex:columnIdx]];
    }


    - (NSData*)dataForColumn:(NSString*)columnName {
        return [self dataForColumnIndex:[self columnIndexForName:columnName]];
    }

    - (NSData*)dataForColumnIndex:(int)columnIdx {

        if (sqlite3_column_type([_statement statement], columnIdx) == SQLITE_NULL || (columnIdx < 0) || columnIdx >= sqlite3_column_count([_statement statement])) {
            return nil;
        }

        const char *dataBuffer = sqlite3_column_blob([_statement statement], columnIdx);
        int dataSize = sqlite3_column_bytes([_statement statement], columnIdx);

        if (dataBuffer == NULL) {
            return nil;
        }

        return [NSData dataWithBytes:(const void *)dataBuffer length:(NSUInteger)dataSize];
    }


    - (NSData*)dataNoCopyForColumn:(NSString*)columnName {
        return [self dataNoCopyForColumnIndex:[self columnIndexForName:columnName]];
    }

    - (NSData*)dataNoCopyForColumnIndex:(int)columnIdx {

        if (sqlite3_column_type([_statement statement], columnIdx) == SQLITE_NULL || (columnIdx < 0) || columnIdx >= sqlite3_column_count([_statement statement])) {
            return nil;
        }

        const char *dataBuffer = sqlite3_column_blob([_statement statement], columnIdx);
        int dataSize = sqlite3_column_bytes([_statement statement], columnIdx);

        NSData *data = [NSData dataWithBytesNoCopy:(void *)dataBuffer length:(NSUInteger)dataSize freeWhenDone:NO];

        return data;
    }


    - (BOOL)columnIndexIsNull:(int)columnIdx {
        return sqlite3_column_type([_statement statement], columnIdx) == SQLITE_NULL;
    }

    - (BOOL)columnIsNull:(NSString*)columnName {
        return [self columnIndexIsNull:[self columnIndexForName:columnName]];
    }

    - (const unsigned char *)UTF8StringForColumnIndex:(int)columnIdx {

        if (sqlite3_column_type([_statement statement], columnIdx) == SQLITE_NULL || (columnIdx < 0) || columnIdx >= sqlite3_column_count([_statement statement])) {
            return nil;
        }

        return sqlite3_column_text([_statement statement], columnIdx);
    }

    - (const unsigned char *)UTF8StringForColumn:(NSString*)columnName {
        return [self UTF8StringForColumnIndex:[self columnIndexForName:columnName]];
    }

    - (const unsigned char *)UTF8StringForColumnName:(NSString*)columnName {
        return [self UTF8StringForColumn:columnName];
    }

    - (id)objectForColumnIndex:(int)columnIdx {
        if (columnIdx < 0 || columnIdx >= sqlite3_column_count([_statement statement])) {
            return nil;
        }

        int columnType = sqlite3_column_type([_statement statement], columnIdx);

        id returnValue = nil;

        if (columnType == SQLITE_INTEGER) {
            returnValue = [NSNumber numberWithLongLong:[self longLongIntForColumnIndex:columnIdx]];
        }
        else if (columnType == SQLITE_FLOAT) {
            returnValue = [NSNumber numberWithDouble:[self doubleForColumnIndex:columnIdx]];
        }
        else if (columnType == SQLITE_BLOB) {
            returnValue = [self dataForColumnIndex:columnIdx];
        }
        else {
            //default to a string for everything else
            returnValue = [self stringForColumnIndex:columnIdx];
        }

        if (returnValue == nil) {
            returnValue = [NSNull null];
        }

        return returnValue;
    }

    - (id)objectForColumnName:(NSString*)columnName {
        return [self objectForColumn:columnName];
    }

    - (id)objectForColumn:(NSString*)columnName {
        return [self objectForColumnIndex:[self columnIndexForName:columnName]];
    }

    // returns autoreleased NSString containing the name of the column in the result set
    - (NSString*)columnNameForIndex:(int)columnIdx {
        return [NSString stringWithUTF8String: sqlite3_column_name([_statement statement], columnIdx)];
    }

    - (id)objectAtIndexedSubscript:(int)columnIdx {
        return [self objectForColumnIndex:columnIdx];
    }

    - (id)objectForKeyedSubscript:(NSString *)columnName {
        return [self objectForColumn:columnName];
    }

    // MARK: Bind

    - (BOOL)bindWithArray:(NSArray*)array orDictionary:(NSDictionary *)dictionary orVAList:(va_list)args {
        [_statement reset];
        return [_parentDB bindStatement:_statement.statement WithArgumentsInArray:array orDictionary:dictionary orVAList:args];
    }

    - (BOOL)bindWithArray:(NSArray*)array {
        return [self bindWithArray:array orDictionary:nil orVAList:nil];
    }

    - (BOOL)bindWithDictionary:(NSDictionary *)dictionary {
        return [self bindWithArray:nil orDictionary:dictionary orVAList:nil];
    }

    @end
*/

}

extension FMResultSet: Hashable {

    public func hash (into hasher: inout Hasher) {
        ObjectIdentifier(self).hash(into: &hasher)
    }
}

public func == (lhs: FMResultSet, rhs: FMResultSet) -> Bool {
    return ObjectIdentifier(lhs) == ObjectIdentifier(rhs)
}
