import Foundation

public enum SQLiteError: Error, LocalizedError {

    case database(message: String)

    case sqliteDone // SQLITE_DONE
    case sqliteRow // SQLITE_ROW

    public var errorDescription: String? {
        switch self {
        case .database (let message):
            return "\(String(describing: self)): \(message)"
        default:
            return "\(String(describing: self))"
        }
    }
}
